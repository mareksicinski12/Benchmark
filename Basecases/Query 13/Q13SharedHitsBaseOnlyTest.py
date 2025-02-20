import time
import json
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

# Configuration
hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'Piotrsql'
port_id = 5432

pg_conn = None
cursor = None

@dataclass
class QueryResult:
    label: str
    query: str
    bench_time: datetime
    exec_time: float      # measured in seconds
    shared_hits: int

results = []

# Define the queries to test
queries = [
    ("Query", """-- Top 10 users by reputation and their post counts
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

    ("Window Function", """-- Using Window Functions
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
    
]

def connect():
    return psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

def accumulate_plan_info(plan_node, accum):
    """
    Recursively walk the 'Plan' tree from the EXPLAIN JSON output,
    accumulating buffer usage and plan statistics.
    """
    buffer_fields = [
        "Shared Hit Blocks",
        "Shared Read Blocks",
        "Shared Dirtied Blocks",
        "Shared Written Blocks",
        "Local Hit Blocks",
        "Local Read Blocks",
        "Local Dirtied Blocks",
        "Local Written Blocks",
        "Temp Read Blocks",
        "Temp Written Blocks"
    ]
    for bf in buffer_fields:
        if bf in plan_node:
            accum[bf] += plan_node[bf]
    # Also accumulate "Actual Rows" and "Actual Loops" (if needed)
    if "Actual Rows" in plan_node:
        accum["Actual Rows"] += plan_node["Actual Rows"]
    if "Actual Loops" in plan_node:
        accum["Actual Loops"] += plan_node["Actual Loops"]
    # Recurse if there are subplans
    if "Plans" in plan_node:
        for subplan in plan_node["Plans"]:
            accumulate_plan_info(subplan, accum)

def extract_shared_hits_from_plan(plan_json):
    """
    Given a JSON plan (as a dict), recursively accumulate and return the
    total number of 'Shared Hit Blocks'.
    """
    # Initialize accumulation dictionary with all needed keys
    accum = {
        "Shared Hit Blocks": 0.0,
        "Shared Read Blocks": 0.0,
        "Shared Dirtied Blocks": 0.0,
        "Shared Written Blocks": 0.0,
        "Local Hit Blocks": 0.0,
        "Local Read Blocks": 0.0,
        "Local Dirtied Blocks": 0.0,
        "Local Written Blocks": 0.0,
        "Temp Read Blocks": 0.0,
        "Temp Written Blocks": 0.0,
        "Actual Rows": 0.0,
        "Actual Loops": 0.0
    }
    plan_root = plan_json["Plan"]
    accumulate_plan_info(plan_root, accum)
    return int(accum["Shared Hit Blocks"])

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
            # Check if explain_output is a string; if so, parse it
            if isinstance(explain_output, str):
                plan_list = json.loads(explain_output)
            else:
                plan_list = explain_output
            # The output is a list with one element (the top-level plan)
            plan_json = plan_list[0]
            exec_time = (time.perf_counter_ns() - start_time) / 1e9  # seconds
            shared_hits = extract_shared_hits_from_plan(plan_json)
            
            results.append(QueryResult(
                label=label,
                query=query,
                bench_time=datetime.now(),
                exec_time=exec_time,
                shared_hits=shared_hits
            ))
    
    # Convert collected results to a Pandas DataFrame
    df = pd.DataFrame([result.__dict__ for result in results])
    
    # Group by query label and compute median, min, and max for exec_time and shared_hits
    agg = df.groupby("label").agg(
        exec_median=("exec_time", "median"),
        exec_min=("exec_time", "min"),
        exec_max=("exec_time", "max"),
        shared_hits_median=("shared_hits", "median"),
        shared_hits_min=("shared_hits", "min"),
        shared_hits_max=("shared_hits", "max")
    ).reset_index()

    # Convert execution time from seconds to milliseconds for plotting
    agg["exec_median_ms"] = agg["exec_median"] * 1000
    agg["exec_min_ms"] = agg["exec_min"] * 1000
    agg["exec_max_ms"] = agg["exec_max"] * 1000

    # Plotting the variation (median with min/max error bars) for execution time and shared hits
    sns.set_context("talk")
    sns.set_style("whitegrid")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
    fig.suptitle("Top 10 users by reputation and their post counts", fontsize=20, fontweight='bold')

    # --- Execution Time Plot ---
    sns.barplot(
        data=agg,
        x="label",
        y="exec_median_ms",
        palette=["#1f77b4"],
        ax=ax1,
        errwidth=2,
        capsize=0.2
    )
    #ax1.set_title("", fontsize=24)
    ax1.set_ylabel("Milliseconds [ms]", fontsize=20)
    ax1.set_xlabel("\nExecution Time\n(Median ± Variation)", fontsize=20)
    ax1.tick_params(axis='both', labelsize=18)
    # Add manual error bars (min to max)
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

    # --- Shared Hits Plot ---
    sns.barplot(
        data=agg,
        x="label",
        y="shared_hits_median",
        palette=["#ff7f0e"],
        ax=ax2,
        errwidth=2,
        capsize=0.2
    )
    #ax2.set_title("", fontsize=24)
    ax2.set_ylabel("Shared Hit Blocks", fontsize=20)
    ax2.set_xlabel("\nShared Hits\n(Median ± Variation)", fontsize=20)
    ax2.tick_params(axis='both', labelsize=18)
    # Only show max values
    for idx, row in agg.iterrows():

        ax2.text(idx, row["shared_hits_max"], f"{row['shared_hits_max']}",
             ha='center', va='bottom', fontsize=16, color='black')  # Add label at max

    plt.tight_layout(pad=3)
    plt.savefig("query_performance_shared_hits_vs_exec_time.png", dpi=300, bbox_inches='tight')
    plt.savefig("query_performance_shared_hits_vs_exec_time.svg", format='svg')
    plt.show()

except Exception as error:
    print(f"Error: {error}")
finally:
    if cursor: cursor.close()
    if pg_conn: pg_conn.close()
