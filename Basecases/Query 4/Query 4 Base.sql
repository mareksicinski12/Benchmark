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
