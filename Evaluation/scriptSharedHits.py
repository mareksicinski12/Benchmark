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

results: list[QueryResult] = []

# Define queries
queries = [
    ("Q1_TagExtraction", """
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

    ("Q2_BadgeCategorization", """
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
]

# Execute queries and collect results
try:
    pg_conn = psycopg2.connect(
        host=hostname, database=database, user=username, password=pwd, port=port_id
    )
    cursor = pg_conn.cursor()

    for label, query in queries:
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        exec_time = time.time() - start_time

        # Fetch PostgreSQL performance metrics
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + query)
        explain_output = cursor.fetchall()
        print(f"EXPLAIN Output for {label}:", explain_output)  # Debugging output

        # Parse relevant metrics (shared hits, shared reads, temp writes)
        shared_hits = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'shared hit=' in line[0])
        shared_reads = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'shared read=' in line[0])
        temp_written = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'temp written=' in line[0])
        
        print(f"{label}: shared_hits={shared_hits}, shared_reads={shared_reads}, temp_written={temp_written}")  # Debugging output
        
        results.append(QueryResult(label, query, datetime.now(), json.dumps(rows), exec_time, shared_hits, shared_reads, temp_written))

    # Save results
    df = pd.DataFrame(results)
    df.to_csv("query_results.csv", index=False)

    # Verify DataFrame before visualization
    print("Aggregated DataFrame:", df[['label', 'shared_hits', 'shared_reads', 'temp_written']])

    # Visualization - Only for shared_hits
    plt.figure(figsize=(12, 6))

    # Filter data for only 'shared_hits'
    df_agg = df[['label', 'shared_hits']].melt(id_vars=['label'], value_vars=['shared_hits'])

    # Debugging output
    print("Filtered DataFrame for Plotting:", df_agg)

    # Plot only shared_hits
    sns.barplot(data=df_agg, x='label', y='value', hue='variable', palette='coolwarm')
    plt.title("Query Performance - Shared Hits Only")
    plt.xlabel("Query Label")
    plt.ylabel("Shared Hits")
    plt.xticks(rotation=45)
    plt.legend(title="Metric Type")
    plt.tight_layout()
    plt.savefig("query_shared_hits.png")
    plt.show()

except Exception as error:
    print(error)
finally:
    if cursor is not None:
        cursor.close()
    if pg_conn is not None:
        pg_conn.close()
