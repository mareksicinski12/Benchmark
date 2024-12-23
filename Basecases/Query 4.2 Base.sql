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
