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

        # Parse relevant metrics (shared hits, shared reads, temp writes)
        shared_hits = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'shared hit=' in line[0])
        shared_reads = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'shared read=' in line[0])
        temp_written = sum(int(line[0].split('=')[-1].strip().split()[0]) for line in explain_output if 'temp written=' in line[0])
        
        results.append(QueryResult(label, query, datetime.now(), json.dumps(rows), exec_time, shared_hits, shared_reads, temp_written))

    # Save results
    df = pd.DataFrame(results)
    df.to_csv("query_results.csv", index=False)

    # Visualization - Only for shared_hits
    plt.figure(figsize=(12, 6))
    
    # Filter data for only 'shared_hits'
    df_agg = df[['label', 'shared_hits']].melt(id_vars=['label'], value_vars=['shared_hits'])
    
    # Improved Plot Formatting
    sns.set_style("whitegrid")
    sns.set_palette("coolwarm")
    ax = sns.barplot(data=df_agg, x='label', y='value', hue='variable')
    ax.set_title("Query Performance - Shared Hits Only", fontsize=14, fontweight='bold')
    ax.set_xlabel("Query Label", fontsize=12)
    ax.set_ylabel("Shared Hits", fontsize=12)
    plt.xticks(rotation=45, fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(title="Metric Type", fontsize=10)
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
