import json
import time
import os
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns


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
    result_set: str
    exec_time: float

results: list[QueryResult] = []


queries = [
    ("Q12_CTE", "WITH AvgPostScore AS (SELECT AVG(p.Score) AS AverageScore FROM posts p) SELECT u.Id AS UserId,u.DisplayName AS UserDisplayName,p.Id AS PostId,p.Title AS PostTitle, p.Score AS PostScore FROM users u JOIN posts p ON u.Id = p.OwnerUserId JOIN AvgPostScore aps ON p.Score > aps.AverageScore WHERE u.Reputation > 10000 AND p.Title IS NOT NULL ORDER BY u.Reputation DESC, p.Score DESC;"),
    ("Q12_SUBQUERY", "WITH AvgPostScore AS (SELECT AVG(p.Score) AS AverageScore FROM posts p ) SELECT u.Id AS UserId, u.DisplayName AS UserDisplayName, p.Id AS PostId, p.Title AS PostTitle, p.Score AS PostScore FROM users u  JOIN posts p ON u.Id = p.OwnerUserId  JOIN (SELECT AVG(p.Score) AS AverageScore FROM posts p) aps ON p.Score > aps.AverageScore WHERE u.Reputation > 10000 AND p.Title IS NOT NULL ORDER BY u.Reputation DESC, p.Score DESC;"),
   
]

def connect():
    return psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

try:
    # Step 1: General Cold Run (not recorded)
    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("CHECKPOINT;")  # Clear buffers
    pg_conn.close()

    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("SET geqo TO off;")  # Disable GEQO

    # Execute all queries once to warm up the cache (general cold run)
    for label, query in queries:
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" + query)
        cursor.fetchall()  # Execute but don't record

    # Step 2: Measure 3 hot runs per query
    os.makedirs("query_plans", exist_ok=True)
    for label, query in queries:
        for run_id in ["hot1", "hot2", "hot3"]:
            bench_start = datetime.now()
            query_start = time.perf_counter_ns()
            
            # Execute and save plan
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" + query)
            result_set = cursor.fetchall()
            
            # Save JSON plan 
            plan_filename = f"query_plans/{label}_{run_id}_plan.json"
            with open(plan_filename, "w") as f:
                json.dump(result_set, f)
            
            query_end = time.perf_counter_ns()
            exec_time = (query_end - query_start) / 1e9  # Convert to seconds
            
            results.append(QueryResult(
                label=f"{label}_{run_id}",
                query=query,
                bench_time=bench_start,
                result_set=json.dumps(result_set),
                exec_time=exec_time
            ))

    # Save results
    df = pd.DataFrame(results)
    df.to_csv("results.csv", index=False)

    # Step 3: Analysis & Visualization
    # Split label into components
    df[["query_id", "sql_feature", "run_type"]] = df["label"].str.split("_", expand=True, n=2)
    
    # Calculate median of hot runs
    df_agg = df.groupby(["query_id", "sql_feature"])["exec_time"].median().reset_index()
    
    # Plot comparison
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df_agg,
        x="query_id",
        y="exec_time",
        hue="sql_feature",
        palette="viridis"
    )
    plt.title("Query Execution Time Comparison (Median of 3 Hot Runs)")
    plt.xlabel("Query ID")
    plt.ylabel("Execution Time (seconds)")
    plt.xticks(rotation=45)
    plt.legend(title="SQL Feature", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig("query_comparison.png")
    plt.show()

except Exception as error:
    print(error)
finally:
    if cursor is not None:
        cursor.close()
    if pg_conn is not None:
        pg_conn.close()
