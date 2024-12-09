-- Find the top 5 users with the highest average score on their posts
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
