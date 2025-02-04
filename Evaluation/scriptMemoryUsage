import json
import time
import os
from dataclasses import dataclass
from datetime import datetime
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

# Define queries
queries = [
    ("Q3", """
        SELECT posts.Id AS PostId, posts.Title, posts.Tags AS OriginalTags,
               json_agg(tags.tag_name) AS TagsAsJSON, COUNT(tags.tag_name) AS TagCount
        FROM posts
        LEFT JOIN LATERAL (
            SELECT match[1] AS tag_name
            FROM regexp_matches(posts.Tags, '<([^<>]+)>', 'g') AS match
        ) tags ON true
        WHERE posts.Tags IS NOT NULL
        GROUP BY posts.Id, posts.Title, posts.Tags
        ORDER BY TagCount DESC;
    """),
    ("Q5", """
        WITH BadgeCounts AS (
            SELECT users.Id AS UserId, users.DisplayName AS UserName,
                   COUNT(badges.Id) AS BadgeCount
            FROM users
            LEFT JOIN badges ON users.Id = badges.UserId
            GROUP BY users.Id, users.DisplayName
        )
        SELECT UserId, UserName, BadgeCount,
               CASE WHEN BadgeCount = 0 THEN 'No Badges'
                    WHEN BadgeCount BETWEEN 1 AND 5 THEN 'Bronze'
                    WHEN BadgeCount BETWEEN 6 AND 15 THEN 'Silver'
                    ELSE 'Gold' END AS BadgeCategory
        FROM BadgeCounts
        ORDER BY BadgeCount DESC;
    """),
    ("Q7", """
        SELECT u.Id AS UserId, u.DisplayName, u.ProfileImageUrl,
               LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) AS UrlSize
        FROM users u
        WHERE u.ProfileImageUrl IS NOT NULL 
              AND u.ProfileImageUrl NOT LIKE 'http://i.stack.imgur.com/%'
              AND LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) < 70;
    """),
    ("Q15", """
        WITH RECURSIVE UserInteractions AS (
            SELECT c.UserId AS User1, p.OwnerUserId AS User2
            FROM comments c
            JOIN posts p ON c.PostId = p.Id
            WHERE c.UserId IS NOT NULL AND p.OwnerUserId IS NOT NULL
            UNION
            SELECT ui.User2 AS User1, p.OwnerUserId AS User2
            FROM UserInteractions ui
            JOIN posts p ON ui.User2 = p.OwnerUserId
        )
        SELECT User1, User2 FROM UserInteractions WHERE User1 = 13 GROUP BY User1, User2;
    """),
]


import json
import time
import os
from dataclasses import dataclass
from datetime import datetime
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

# Execute queries and collect results
try:
    pg_conn = psycopg2.connect(
        host=hostname, database=database, user=username, password=pwd, port=port_id
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

    for label, query in queries:
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        exec_time = time.time() - start_time

        # Fetch PostgreSQL performance metrics
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + query)
        explain_output = cursor.fetchall()
        explain_text = " ".join(line[0] for line in explain_output)

        # Parse relevant metrics
        def extract_metric(metric_name, text):
            import re
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

        results.append(QueryResult(
            label, query, datetime.now(), json.dumps(rows), exec_time, 
            shared_hits, shared_reads, temp_written, temp_blks_written, 
            work_mem, shared_buffers, cache_hit_ratio, memory_usage
        ))

    # Save results
    df = pd.DataFrame(results)
    df.to_csv("query_results.csv", index=False)

    # Convert memory usage to MB
    df['memory_usage_MB'] = df['memory_usage'] / (1024 * 1024)

    # Print extracted memory-related metrics
    print(df[['label', 'memory_usage_MB']])

    # Visualization - Only Memory Usage in MB with Labels on Bars
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
    
    plt.savefig("query_memory_usage.png")
    plt.show()

except Exception as error:
    print("Error:", error)
finally:
    if cursor is not None:
        cursor.close()
    if pg_conn is not None:
        pg_conn.close()
