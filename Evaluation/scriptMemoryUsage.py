import json
import time
import os
from dataclasses import dataclass
from datetime import datetime
import decimal
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

# Database connection settings
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
    result_set: str
    exec_time: float
    shared_hits: int
    shared_reads: int
    temp_written: int
    temp_blks_written: int
    work_mem: str
    shared_buffers: str
    cache_hit_ratio: float
    memory_usage: int

results: list[QueryResult] = []

# Custom function to convert Decimal to float before JSON serialization
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

# Define queries
queries = [
    ("Q10", """
-- Analyze post activity by hour, including totals and averages
WITH PostActivityByHour AS (
    SELECT 
        EXTRACT(HOUR FROM posts.CreationDate) AS CreationHour,
        COUNT(posts.Id) AS TotalPosts,
        ROUND(AVG(posts.Score), 2) AS AverageScore
    FROM 
        posts
    GROUP BY 
        GROUPING SETS (
            (EXTRACT(HOUR FROM posts.CreationDate)),
            ()
        )
)
-- Select and categorize hourly activity
SELECT 
    CreationHour,
    TotalPosts,
    AverageScore,
    CASE 
        WHEN CreationHour IS NULL THEN 1 
        ELSE 0 
    END AS IsTotal
FROM 
    PostActivityByHour
ORDER BY 
    CASE WHEN CreationHour IS NULL THEN 1 ELSE 0 END,
    CreationHour;
    """),
    ("Q4 GROUPING SETS", """
        -- Using GROUPING SETS for Aggregation
SELECT 
    CASE 
        WHEN GROUPING(EXTRACT(HOUR FROM posts.CreationDate)) = 1 THEN NULL
        ELSE EXTRACT(HOUR FROM posts.CreationDate)
    END AS CreationHour,
    COUNT(posts.Id) AS TotalPosts,
    ROUND(AVG(posts.Score), 2) AS AverageScore,
    GROUPING(EXTRACT(HOUR FROM posts.CreationDate)) AS IsTotal
FROM 
    posts
GROUP BY 
    GROUPING SETS (
        (EXTRACT(HOUR FROM posts.CreationDate)),
        ()
    )
ORDER BY 
    IsTotal,
    CreationHour;
    """),
    ("Q4 UNION ALL", """
        -- Using a UNION for Total and Hourly Aggregation
SELECT 
    EXTRACT(HOUR FROM posts.CreationDate) AS CreationHour,
    COUNT(posts.Id) AS TotalPosts,
    ROUND(AVG(posts.Score), 2) AS AverageScore,
    0 AS IsTotal
FROM 
    posts
GROUP BY 
    EXTRACT(HOUR FROM posts.CreationDate)

UNION ALL

SELECT 
    NULL AS CreationHour,
    COUNT(posts.Id) AS TotalPosts,
    ROUND(AVG(posts.Score), 2) AS AverageScore,
    1 AS IsTotal
FROM 
    posts

ORDER BY 
    IsTotal,
    CreationHour;
    """),
]

try:
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cursor = pg_conn.cursor()

    # Fetch system-wide memory settings
    cursor.execute("SHOW work_mem;")
    work_mem = cursor.fetchone()[0]
    cursor.execute("SHOW shared_buffers;")
    shared_buffers = cursor.fetchone()[0]

    # Fetch cache hit ratio
    cursor.execute("""
        SELECT ROUND(100 * blks_hit::numeric / NULLIF(blks_hit + blks_read, 0), 2)
        FROM pg_stat_database
        WHERE datname = current_database();
    """)
    cache_hit_ratio = cursor.fetchone()[0]

    # Execute each query and gather performance + result metrics
    for label, query in queries:
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        exec_time = time.time() - start_time

        # Explain for performance metrics
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + query)
        explain_output = cursor.fetchall()
        explain_text = " ".join(line[0] for line in explain_output)

        # Parse relevant metrics from EXPLAIN output
        import re
        def extract_metric(metric_name, text):
            match = re.search(fr'{metric_name}=(\d+)', text)
            return int(match.group(1)) if match else 0

        shared_hits = extract_metric('shared hit', explain_text)
        shared_reads = extract_metric('shared read', explain_text)
        temp_written = extract_metric('temp written', explain_text)
        temp_blks_written = extract_metric('Buffers: temp written', explain_text)

        # Fetch active memory usage for the backend
        cursor.execute("""
            SELECT SUM(total_bytes) 
            FROM pg_backend_memory_contexts;
        """)
        memory_usage = cursor.fetchone()[0]

        # Serialize rows with a custom Decimal converter
        rows_json = json.dumps(rows, default=decimal_default)

        results.append(QueryResult(
            label=label,
            query=query,
            bench_time=datetime.now(),
            result_set=rows_json,
            exec_time=exec_time,
            shared_hits=shared_hits,
            shared_reads=shared_reads,
            temp_written=temp_written,
            temp_blks_written=temp_blks_written,
            work_mem=work_mem,
            shared_buffers=shared_buffers,
            cache_hit_ratio=cache_hit_ratio,
            memory_usage=memory_usage
        ))

    # Convert to DataFrame
    df = pd.DataFrame(results)
    # Save results
    df.to_csv("query_results.csv", index=False)

    # Convert memory usage to MB
    df['memory_usage_MB'] = df['memory_usage'] / (1024 * 1024)

    # Print memory usage
    print(df[['label', 'memory_usage_MB']])

    # Visualization - Memory Usage in MB
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    ax = sns.barplot(data=df, x='label', y='memory_usage_MB', color='orange', alpha=0.8)
    
    # Add value labels on top of bars
    for p in ax.patches:
        ax.annotate(f'{p.get_height():.2f} MB', 
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax.set_title("Query Memory Usage", fontsize=14, fontweight='bold')
    ax.set_xlabel("Query Label", fontsize=12)
    ax.set_ylabel("Memory Usage (MB)", fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    plt.savefig("query4_memory_usage.png")
    plt.show()

except Exception as error:
    print("Error:", error)
finally:
    if cursor is not None:
        cursor.close()
    if pg_conn is not None:
        pg_conn.close()
