import time
import json
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'postgres'
port_id = 5432

pg_conn = None
cursor = None

@dataclass
class QueryResult:
    label: str
    query: str
    bench_time: datetime
    exec_time: float      
    estimated_cost: float  

results = []

# Define the queries to test 
queries = [
    ("Simple", """-- Top 10 users by reputation and their post counts
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COUNT(p.Id) AS PostCount
FROM 
    users u
LEFT JOIN 
    posts p ON u.Id = p.OwnerUserId
GROUP BY 
    u.Id, u.DisplayName, u.Reputation
ORDER BY 
    u.Reputation DESC
LIMIT 10;
    """),

    ("Window\nFunction", """-- Using Window Functions
WITH RankedUsers AS (
    SELECT 
        u.Id,
        u.DisplayName,
        u.Reputation,
        COUNT(p.Id) AS PostCount,
        ROW_NUMBER() OVER (ORDER BY u.Reputation DESC) AS RowNum
    FROM 
        users u
    LEFT JOIN 
        posts p ON u.Id = p.OwnerUserId
    GROUP BY 
        u.Id, u.DisplayName, u.Reputation
)
SELECT 
    Id,
    DisplayName,
    Reputation,
    PostCount
FROM 
    RankedUsers
WHERE 
    RowNum <= 10;
    """),

    ("Subquery", """-- Using Subquery to Pre-Aggregate Post Counts
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COALESCE(PostCounts.PostCount, 0) AS PostCount
FROM 
    users u
LEFT JOIN (
    SELECT 
        p.OwnerUserId,
        COUNT(p.Id) AS PostCount
    FROM 
        posts p
    GROUP BY 
        p.OwnerUserId
) PostCounts ON u.Id = PostCounts.OwnerUserId
ORDER BY 
    u.Reputation DESC
LIMIT 10;
    """),
    ("Advanced", """-- CTE for post-related metrics: calculates post count, cumulative score, and post date range for each user
WITH UserMetrics AS (
    SELECT 
        p.OwnerUserId AS UserId,
        COUNT(p.Id) AS PostCount,
        SUM(p.Score) AS TotalScore,
        MIN(p.CreationDate) AS FirstPostDate,
        MAX(p.CreationDate) AS LastPostDate
    FROM 
        posts p
    GROUP BY 
        p.OwnerUserId
),

-- CTE for user reputation rankings
UserRankings AS (
    SELECT 
        u.Id AS UserId,
        u.Reputation,
        RANK() OVER (ORDER BY u.Reputation DESC) AS ReputationRank
    FROM 
        users u
)

-- top 10 users with detailed metrics
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COALESCE(um.PostCount, 0) AS PostCount,  -- Total posts
    COALESCE(um.TotalScore, 0) AS TotalScore, -- Cumulative post score

    -- Best post title
    COALESCE(
        CONCAT('BEST POST: ', (
            SELECT 
                COALESCE(p1.Title, 'No Title')
            FROM 
                posts p1
            WHERE 
                p1.OwnerUserId = u.Id
            ORDER BY 
                p1.Score DESC
            LIMIT 1
        )),
        'No Title'
    ) AS BestPost,

    -- Membership duration in years and months
    CONCAT(
        u.DisplayName, 
        ' is with us for: ',
        EXTRACT(YEAR FROM AGE(NOW(), u.CreationDate))::TEXT, 
        ' years',
        CASE
            WHEN EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate)) > 0 THEN
                CONCAT(' and ', EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate))::TEXT, ' months')
            ELSE
                ''
        END,
        ' already!'
    ) AS Membership,

    -- Total badges earned
    (SELECT COUNT(*) FROM badges b WHERE b.UserId = u.Id) AS BadgeCount,

    -- Reputation rank
    ur.ReputationRank,

    -- Post date range
    COALESCE(um.FirstPostDate, NULL) AS FirstPostDate,
    COALESCE(um.LastPostDate, NULL) AS LastPostDate,

    -- Upvote percentage
    ROUND(
        CASE 
            WHEN (u.UpVotes + u.DownVotes) > 0 THEN 
                (u.UpVotes::NUMERIC / (u.UpVotes + u.DownVotes)) * 100
            ELSE 
                0
        END, 2
    ) AS UpvotePercentage
FROM 
    users u
LEFT JOIN 
    UserMetrics um ON u.Id = um.UserId
LEFT JOIN 
    UserRankings ur ON u.Id = ur.UserId
ORDER BY 
    u.Reputation DESC
LIMIT 10;"""),
    
]

def connect():
    return psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

def extract_estimated_cost(plan_json):
    """Extract total estimated cost from query plan"""
    return plan_json["Plan"]["Total Cost"]

try:
    # Initial database setup
    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("CHECKPOINT;")
    pg_conn.close()

    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("SET geqo TO off;")

    # For each query, perform warmup runs then measurement runs
    for label, query in queries:
        # Warmup runs (to ensure caches are warmed)
        for _ in range(3):
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query)
            cursor.fetchall()
        
        # Measurement runs
        for run in range(11):
            start_time = time.perf_counter_ns()
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query)
            explain_output = cursor.fetchone()[0]
            if isinstance(explain_output, str):
                plan_list = json.loads(explain_output)
            else:
                plan_list = explain_output
            plan_json = plan_list[0]
            exec_time = (time.perf_counter_ns() - start_time) / 1e9  # seconds
            estimated_cost = extract_estimated_cost(plan_json)
            
            results.append(QueryResult(
                label=label,
                query=query,
                bench_time=datetime.now(),
                exec_time=exec_time,
                estimated_cost=estimated_cost
            ))
    
    # Convert collected results to a Pandas DataFrame
    df = pd.DataFrame([result.__dict__ for result in results])
    
    # Group by query label and compute median, min, and max
    agg = df.groupby("label").agg(
        exec_median=("exec_time", "median"),
        exec_min=("exec_time", "min"),
        exec_max=("exec_time", "max"),
        cost_median=("estimated_cost", "median"),
        cost_min=("estimated_cost", "min"),
        cost_max=("estimated_cost", "max")
    ).reset_index()

    # Convert execution time from seconds to milliseconds for plotting
    agg["exec_median_ms"] = agg["exec_median"] * 1000
    agg["exec_min_ms"] = agg["exec_min"] * 1000
    agg["exec_max_ms"] = agg["exec_max"] * 1000

    # Scale estimated costs to thousands for plotting
    agg["cost_median_k"] = agg["cost_median"] / 1000
    agg["cost_max_k"] = agg["cost_max"] / 1000

    # Plotting the variation (median with min/max error bars)
    sns.set_context("talk")
    sns.set_style("whitegrid")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
    fig.suptitle("Top 10 users by reputation and their post counts", fontsize=20, fontweight='bold')

    # Execution Time Plot 
    sns.barplot(
        data=agg,
        x="label",
        y="exec_median_ms",
        palette=["#ff7f0e"],
        ax=ax1,
        errwidth=2,
        capsize=0.2
    )
    ax1.set_ylabel("Milliseconds [ms]", fontsize=20)
    ax1.set_xlabel("\nExecution Time\n(Median ± Variation)", fontsize=20)
    ax1.tick_params(axis='both', labelsize=18)
    
    for idx, row in agg.iterrows():
        ax1.plot([idx, idx], [row["exec_min_ms"], row["exec_max_ms"]],
                 color='black', linewidth=3)
        ax1.plot([idx-0.2, idx+0.2], [row["exec_min_ms"], row["exec_min_ms"]],
                 color='black', linewidth=3)
        ax1.plot([idx-0.2, idx+0.2], [row["exec_max_ms"], row["exec_max_ms"]],
                 color='black', linewidth=3)
        ax1.text(idx, row["exec_min_ms"], f"{row['exec_min_ms']:.1f}",
                 ha='center', va='top', fontsize=16, color='black')
        ax1.text(idx, row["exec_max_ms"], f"{row['exec_max_ms']:.1f}",
                 ha='center', va='bottom', fontsize=16, color='black')

    # Estimated Cost Plot 
    sns.barplot(
        data=agg,
        x="label",
        y="cost_median_k",
        palette=["#1f77b4"],
        ax=ax2,
        errwidth=2,
        capsize=0.2
    )
    ax2.set_ylabel("Computational Units", fontsize=20)
    ax2.set_xlabel("\nPlanner's Estimated Total Cost\n(Median)", fontsize=20)
    ax2.tick_params(axis='both', labelsize=18)
    
    for idx, row in agg.iterrows():
        ax2.text(idx, row["cost_max_k"], f"{row['cost_max_k']:.1f}k",
                 ha='center', va='bottom', fontsize=16, color='black')

    plt.tight_layout(pad=3)
    plt.savefig("Q13_ADVANCEDquery_performance_estimated_cost_vs_exec_time.png", dpi=300, bbox_inches='tight')
    plt.savefig("Q13_ADVANCEDquery_performance_estimated_cost_vs_exec_time.svg", format='svg')
    plt.show()

except Exception as error:
    print(f"Error: {error}")
finally:
    if cursor:
        cursor.close()
    if pg_conn:
        pg_conn.close()
