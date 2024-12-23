-- Using a Subquery to Calculate Average Scores
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
