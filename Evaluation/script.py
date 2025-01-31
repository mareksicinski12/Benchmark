#tu trzeba tylko zmienic ze zamiast 1 cold run i 2 hot runs to bedzie robil 3 hot runs i z tego wyciagal mediane bo inaczej to dla pierwszej query cold run jest bardzo zawyzony 


import json
import time
import os
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
    result_set: str
    exec_time: float

results: list[QueryResult] = []


queries = [
    ("Q12_CTE", "WITH AvgPostScore AS (SELECT AVG(p.Score) AS AverageScore FROM posts p) SELECT u.Id AS UserId,u.DisplayName AS UserDisplayName,p.Id AS PostId,p.Title AS PostTitle, p.Score AS PostScore FROM users u JOIN posts p ON u.Id = p.OwnerUserId JOIN AvgPostScore aps ON p.Score > aps.AverageScore WHERE u.Reputation > 10000 AND p.Title IS NOT NULL ORDER BY u.Reputation DESC, p.Score DESC;"),
    ("Q12_SUBQUERY", "WITH AvgPostScore AS (SELECT AVG(p.Score) AS AverageScore FROM posts p ) SELECT u.Id AS UserId, u.DisplayName AS UserDisplayName, p.Id AS PostId, p.Title AS PostTitle, p.Score AS PostScore FROM users u  JOIN posts p ON u.Id = p.OwnerUserId  JOIN (SELECT AVG(p.Score) AS AverageScore FROM posts p) aps ON p.Score > aps.AverageScore WHERE u.Reputation > 10000 AND p.Title IS NOT NULL ORDER BY u.Reputation DESC, p.Score DESC;"),
    # ... Add all queries
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
    # Cold run setup
    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("CHECKPOINT;")  # Flush buffers to disk
    pg_conn.close()

    # Reconnect for cold runs
    pg_conn = connect()  
    cursor = pg_conn.cursor()
    cursor.execute("SET geqo TO off;")

    os.makedirs("query_plans", exist_ok=True)

    for label, query in queries:
        # Cold run
        bench_start = datetime.now()
        query_start = time.perf_counter_ns()
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" + query)
        result_set = cursor.fetchall()
        query_end = time.perf_counter_ns()
        
        # Save cold run plan
        with open(f"query_plans/{label}_cold_plan.json", "w") as f:
            json.dump(result_set, f)
        
        results.append(QueryResult(
            label=f"{label}_cold",
            query=query,
            bench_time=bench_start,
            result_set=json.dumps(result_set),
            exec_time=(query_end - query_start) / 1e9  # Convert to seconds
        ))

        # Warm runs (reuse connection)
        for warm_id in ["warm1", "warm2"]:
            bench_start = datetime.now()
            query_start = time.perf_counter_ns()
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" + query)
            result_set = cursor.fetchall()
            query_end = time.perf_counter_ns()
            
            results.append(QueryResult(
                label=f"{label}_{warm_id}",
                query=query,
                bench_time=bench_start,
                result_set=json.dumps(result_set),
                exec_time=(query_end - query_start) / 1e9
            ))

    df = pd.DataFrame(results)
    df.to_csv("results.csv", index=False)

# ===== Visualization Code =====
    # Split labels into components
    df[["query_id", "sql_feature", "run_type"]] = df["label"].str.split("_", expand=True, n=2)
    
    # Aggregate data (median of warm runs)
    df_agg = df.groupby(["query_id", "sql_feature"]).agg(
        median_exec_time=("exec_time", "median")
    ).reset_index()

    # Plot grouped bar chart
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df_agg,
        x="query_id",
        y="median_exec_time",
        hue="sql_feature",
        palette="viridis"
    )
    plt.title("Comparison of Query Execution Times by SQL Feature")
    plt.xlabel("Query ID")
    plt.ylabel("Median Execution Time (seconds)")
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
