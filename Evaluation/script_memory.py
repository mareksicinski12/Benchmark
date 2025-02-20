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
    exec_time: float
    memory_usage_kb: float

results = []

queries = [
    ("Q2a", """
    SELECT 
        users.Id AS UserId,
        users.DisplayName AS UserName,
        AVG(posts.Score) AS AverageScore
    FROM 
        users
    JOIN 
        posts ON users.Id = posts.OwnerUserId
    GROUP BY 
        users.Id, users.DisplayName
    ORDER BY 
        AverageScore DESC
    LIMIT 5;
    """),

    ("Q2b", """
    SELECT 
        UserId,
        UserName,
        AverageScore
    FROM (
    SELECT 
        users.Id AS UserId,
        users.DisplayName AS UserName,
        AVG(posts.Score) AS AverageScore
    FROM 
        users
    JOIN 
        posts ON users.Id = posts.OwnerUserId
    GROUP BY 
        users.Id, users.DisplayName
        ) UserScores
    ORDER BY 
        AverageScore DESC
    LIMIT 5;
    """)
]

def connect():
    return psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

def measure_memory():
    cursor.execute("SELECT SUM(total_bytes) FROM pg_backend_memory_contexts;")
    return float(cursor.fetchone()[0]) / 1024  # Convert to KB

try:
    # Database setup
    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("CHECKPOINT;")
    pg_conn.close()
    
    pg_conn = connect()
    cursor = pg_conn.cursor()
    cursor.execute("SET geqo TO off;")

    # Warmup and measurement (3 hot runs)
    for label, query in queries:
        for _ in range(3):  # Warmup
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + query)
            cursor.fetchall()
        
        for run in range(3):  # Actual measurements
            start_time = time.perf_counter_ns()
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + query)
            result_set = cursor.fetchall()
            exec_time = (time.perf_counter_ns() - start_time) / 1e9
            memory = measure_memory()
            
            results.append(QueryResult(
                label=label,
                query=query,
                bench_time=datetime.now(),
                exec_time=exec_time,
                memory_usage_kb=memory
            ))

    # Convert QueryResult objects to dictionary for DataFrame
    df = pd.DataFrame([result.__dict__ for result in results])

    agg = df.groupby("label").agg(
        exec_median=("exec_time", "median"),
        exec_min=("exec_time", "min"),
        exec_max=("exec_time", "max"),
        mem_median=("memory_usage_kb", "median"),
        mem_min=("memory_usage_kb", "min"),
        mem_max=("memory_usage_kb", "max")
    ).reset_index()

    # Convert units
    agg["exec_median_ms"] = agg["exec_median"] * 1000
    agg["exec_min_ms"] = agg["exec_min"] * 1000
    agg["exec_max_ms"] = agg["exec_max"] * 1000
    agg["mem_median_mb"] = agg["mem_median"] / 1024
    agg["mem_min_mb"] = agg["mem_min"] / 1024
    agg["mem_max_mb"] = agg["mem_max"] / 1024

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

    # Execution Time Plot
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

    # Add error bars manually
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

    # Memory Usage Plot
    sns.barplot(
        data=agg,
        x="label",
        y="mem_median_mb",
        palette=["#ff7f0e"],
        ax=ax2,
        errwidth=2,
        capsize=0.2
    )
    ax2.set_title("Memory Usage\n(Median ± Variation)", fontsize=24)
    ax2.set_ylabel("Megabytes [MB]", fontsize=20)
    ax2.set_xlabel("Query", fontsize=20)
    ax2.tick_params(axis='both', labelsize=18)

    # Add error bars manually
    for idx, row in agg.iterrows():
        ax2.plot([idx, idx], [row["mem_min_mb"], row["mem_max_mb"]], 
                color='black', linewidth=3)
        ax2.plot([idx-0.2, idx+0.2], [row["mem_min_mb"], row["mem_min_mb"]], 
                color='black', linewidth=3)
        ax2.plot([idx-0.2, idx+0.2], [row["mem_max_mb"], row["mem_max_mb"]], 
                color='black', linewidth=3)
        ax2.text(idx, row["mem_min_mb"], f"{row['mem_min_mb']:.1f}",
                ha='center', va='top', fontsize=16, color='black')
        ax2.text(idx, row["mem_max_mb"], f"{row['mem_max_mb']:.1f}",
                ha='center', va='bottom', fontsize=16, color='black')

    # Final adjustments
    plt.tight_layout(pad=3)
    plt.savefig("query_performance_presentation.png", dpi=300, bbox_inches='tight')
    plt.savefig("query_performance_presentation.svg", format='svg')
    plt.show()

except Exception as error:
    print(f"Error: {error}")
finally:
    if cursor: cursor.close()
    if pg_conn: pg_conn.close()
