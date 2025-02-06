import json
import time
import re
import os
import decimal
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

pd.set_option('display.max_columns', None)    # No column limit
pd.set_option('display.width', 200)          # Wider console for better visibility

# ======================
# Database Configuration
# ======================
hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'Piotrsql'
port_id = 5432

pg_conn = None
cursor = None

# ====================
# Data Class for Stats
# ====================
@dataclass
class QueryResult:
    label: str
    query: str
    bench_time: datetime

    # Basic results
    result_set: str
    exec_time: float  # Real measured time

    # Summarized plan metrics
    plan_startup_cost: float
    plan_total_cost: float

    # PostgreSQL timing from the plan
    planning_time: float
    execution_time_plan: float  # "Execution Time" from the plan

    # Actual cardinality
    actual_rows: float
    actual_loops: float

    # Aggregated buffers
    shared_hit_blocks: int
    shared_read_blocks: int
    shared_dirtied_blocks: int
    shared_written_blocks: int
    local_hit_blocks: int
    local_read_blocks: int
    local_dirtied_blocks: int
    local_written_blocks: int
    temp_read_blocks: int
    temp_written_blocks: int

    # Additional system metrics
    memory_usage: int
    cache_hit_ratio: float
    work_mem: str
    shared_buffers: str


# Keep a list of results
results: list[QueryResult] = []

# =========================================
# Utility: Convert Decimal -> float for JSON
# =========================================
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


# =====================================================
# Recursive plan traversal to accumulate buffer usage
# =====================================================
def accumulate_plan_info(plan_node: Dict[str, Any], accum: Dict[str, float]) -> None:
    """
    Recursively walk the 'Plan' tree from EXPLAIN JSON output,
    accumulating buffer usage and plan row counts, loops, etc.
    """
    # Buffer fields in the JSON plan (may or may not exist)
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

    # Accumulate if present in the current node
    for bf in buffer_fields:
        if bf in plan_node:
            accum[bf] += plan_node[bf]

    # Also get "Actual Rows" and "Actual Loops"
    if "Actual Rows" in plan_node:
        accum["Actual Rows"] += plan_node["Actual Rows"]
    if "Actual Loops" in plan_node:
        accum["Actual Loops"] += plan_node["Actual Loops"]

    # If this node has children, recurse
    if "Plans" in plan_node:
        for subplan in plan_node["Plans"]:
            accumulate_plan_info(subplan, accum)


# ========================
# Define your test queries
# ========================
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


try:
    # =========================
    # Connect to PostgreSQL
    # =========================
    pg_conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cursor = pg_conn.cursor()

    # Grab memory settings
    cursor.execute("SHOW work_mem;")
    work_mem = cursor.fetchone()[0]

    cursor.execute("SHOW shared_buffers;")
    shared_buffers = cursor.fetchone()[0]

    # Cache hit ratio
    cursor.execute("""
        SELECT ROUND(100 * blks_hit::numeric / NULLIF(blks_hit + blks_read, 0), 2)
        FROM pg_stat_database
        WHERE datname = current_database();
    """)
    cache_hit_ratio = cursor.fetchone()[0]

    # ================
    # Main Query Loop
    # ================
    for label, query in queries:
        # 1) Measure "real" execution time
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        real_exec_time = time.time() - start_time

        # 2) Fetch an EXPLAIN JSON plan
        #    (ANALYZE, BUFFERS => includes timing & buffer usage)
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query)
        explain_output = cursor.fetchone()[0]  # Single row with JSON text
        plan_json = explain_output[0]  # Top-level list of length 1

        # Top-level plan info
        plan_root = plan_json["Plan"]
        startup_cost = plan_root.get("Startup Cost", 0.0)
        total_cost = plan_root.get("Total Cost", 0.0)
        planning_time = plan_json.get("Planning Time", 0.0)
        execution_time_plan = plan_json.get("Execution Time", 0.0)

        # Prepare accumulation dictionary for plan-level stats
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

        # 3) Recursively walk plan to accumulate buffer usage
        accumulate_plan_info(plan_root, accum)

        # 4) Check memory usage (backend memory contexts)
        cursor.execute("SELECT SUM(total_bytes) FROM pg_backend_memory_contexts;")
        memory_usage = cursor.fetchone()[0]

        # 5) Convert query result to JSON if needed
        rows_json = json.dumps(rows, default=decimal_default)

        # 6) Store everything in our results list
        results.append(
            QueryResult(
                label=label,
                query=query,
                bench_time=datetime.now(),
                result_set=rows_json,
                exec_time=real_exec_time,
                plan_startup_cost=startup_cost,
                plan_total_cost=total_cost,
                planning_time=planning_time,
                execution_time_plan=execution_time_plan,
                actual_rows=accum["Actual Rows"],
                actual_loops=accum["Actual Loops"],
                shared_hit_blocks=int(accum["Shared Hit Blocks"]),
                shared_read_blocks=int(accum["Shared Read Blocks"]),
                shared_dirtied_blocks=int(accum["Shared Dirtied Blocks"]),
                shared_written_blocks=int(accum["Shared Written Blocks"]),
                local_hit_blocks=int(accum["Local Hit Blocks"]),
                local_read_blocks=int(accum["Local Read Blocks"]),
                local_dirtied_blocks=int(accum["Local Dirtied Blocks"]),
                local_written_blocks=int(accum["Local Written Blocks"]),
                temp_read_blocks=int(accum["Temp Read Blocks"]),
                temp_written_blocks=int(accum["Temp Written Blocks"]),
                memory_usage=memory_usage,
                cache_hit_ratio=cache_hit_ratio,
                work_mem=work_mem,
                shared_buffers=shared_buffers
            )
        )

    # ===========================
    # Convert results to DataFrame
    # ===========================
    df = pd.DataFrame(results)
    # Optionally convert memory usage to MB
    df["memory_usage_mb"] = df["memory_usage"] / (1024 * 1024)

    # Save to CSV
    df.to_csv("comprehensive_query_metrics.csv", index=False)

    # Print a subset of columns to confirm
    print(df[[
        "label", 
        "exec_time", 
        "plan_startup_cost", 
        "plan_total_cost",
        "planning_time",
        "execution_time_plan",
        "shared_hit_blocks", 
        "shared_read_blocks", 
        "temp_written_blocks",
        "memory_usage_mb"
    ]])

    # =============
    # Example Plot
    # =============
    sns.set_style("whitegrid")

    # For instance, compare real exec_time vs. plan_total_cost
    plt.figure(figsize=(8, 5))
    sns.scatterplot(
        data=df,
        x="plan_total_cost",
        y="exec_time",
        hue="label",
        s=100
    )
    plt.title("Plan Total Cost vs. Real Execution Time")
    plt.xlabel("Plan Total Cost")
    plt.ylabel("Real Execution Time (s)")
    plt.tight_layout()
    plt.savefig("plan_cost_vs_real_exec_time.png")
    plt.show()

except Exception as error:
    print("Error:", error)

finally:
    if cursor is not None:
        cursor.close()
    if pg_conn is not None:
        pg_conn.close()
