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
