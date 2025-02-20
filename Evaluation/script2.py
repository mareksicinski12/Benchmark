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
database = 'kp'
username = 'postgres'
pwd = '8c11841ab9'
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
    ("Q3", """
        SELECT 
            posts.Id AS PostId,
            posts.Title,
            posts.Tags AS OriginalTags,
            json_agg(tags.tag_name) AS TagsAsJSON,
            COUNT(tags.tag_name) AS TagCount
        FROM 
            posts
        LEFT JOIN 
            LATERAL (
                SELECT match[1] AS tag_name
                FROM regexp_matches(posts.Tags, '<([^<>]+)>', 'g') AS match
            ) tags ON true
        WHERE 
            posts.Tags IS NOT NULL
        GROUP BY 
            posts.Id, posts.Title, posts.Tags
        ORDER BY 
            TagCount DESC;
    """),

    ("Q5", """
        WITH BadgeCounts AS (
            SELECT 
                users.Id AS UserId,
                users.DisplayName AS UserName,
                COUNT(badges.Id) AS BadgeCount
            FROM 
                users
            LEFT JOIN 
                badges ON users.Id = badges.UserId
            GROUP BY 
                users.Id, users.DisplayName
        )
        SELECT 
            UserId,
            UserName,
            BadgeCount,
            CASE 
                WHEN BadgeCount = 0 THEN 'No Badges'
                WHEN BadgeCount BETWEEN 1 AND 5 THEN 'Bronze'
                WHEN BadgeCount BETWEEN 6 AND 15 THEN 'Silver'
                ELSE 'Gold'
            END AS BadgeCategory
        FROM 
            BadgeCounts
        ORDER BY 
            BadgeCount DESC;
    """),

    ("Q7", """
        SELECT 
            u.Id AS UserId,
            u.DisplayName,
            u.ProfileImageUrl,
            LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) AS UrlSize
        FROM 
            users u
        WHERE 
            u.ProfileImageUrl IS NOT NULL 
            AND u.ProfileImageUrl NOT LIKE 'http://i.stack.imgur.com/%'
            AND LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) < 70
    """),

    ("Q15", """
        WITH RECURSIVE UserInteractions AS (
            SELECT 
                c.UserId AS User1, p.OwnerUserId AS User2
            FROM 
                comments c
            JOIN 
                posts p ON c.PostId = p.Id
            WHERE 
                c.UserId IS NOT NULL 
                AND p.OwnerUserId IS NOT NULL
            UNION 
            SELECT 
                ui.User2 AS User1, p.OwnerUserId AS User2
            FROM 
                UserInteractions ui
            JOIN 
                posts p ON ui.User2 = p.OwnerUserId
        )
        SELECT  User1, User2
        FROM UserInteractions
        WHERE User1 = 13
        GROUP BY User1, User2
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
        for run in range(3):
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
    ax1.set_title("Execution Time\n(Median ± Variation)", fontsize=24)
    ax1.set_ylabel("Milliseconds [ms]", fontsize=20)
    ax1.set_xlabel("Query", fontsize=20)
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
    ax2.set_title("Shared Hits\n(Median ± Variation)", fontsize=24)
    ax2.set_ylabel("Shared Hit Blocks", fontsize=20)
    ax2.set_xlabel("Query", fontsize=20)
    ax2.tick_params(axis='both', labelsize=18)
    # Add manual error bars (min to max)
    for idx, row in agg.iterrows():
        ax2.plot([idx, idx], [row["shared_hits_min"], row["shared_hits_max"]],
                 color='black', linewidth=3)
        ax2.plot([idx-0.2, idx+0.2], [row["shared_hits_min"], row["shared_hits_min"]],
                 color='black', linewidth=3)
        ax2.plot([idx-0.2, idx+0.2], [row["shared_hits_max"], row["shared_hits_max"]],
                 color='black', linewidth=3)
        ax2.text(idx, row["shared_hits_min"], f"{row['shared_hits_min']}",
                 ha='center', va='top', fontsize=16, color='black')
        ax2.text(idx, row["shared_hits_max"], f"{row['shared_hits_max']}",
                 ha='center', va='bottom', fontsize=16, color='black')

    plt.tight_layout(pad=3)
    plt.savefig("query_performance_shared_hits_vs_exec_time.png", dpi=300, bbox_inches='tight')
    plt.savefig("query_performance_shared_hits_vs_exec_time.svg", format='svg')
    plt.show()

except Exception as error:
    print(f"Error: {error}")
finally:
    if cursor: cursor.close()
    if pg_conn: pg_conn.close()
