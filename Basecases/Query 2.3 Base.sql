-- Using a Window Function
SELECT 
    UserId,
    UserName,
    AverageScore
FROM (
    SELECT 
        users.Id AS UserId,
        users.DisplayName AS UserName,
        AVG(posts.Score) AS AverageScore,
        RANK() OVER (ORDER BY AVG(posts.Score) DESC) AS Rank
    FROM 
        users
    JOIN 
        posts ON users.Id = posts.OwnerUserId
    GROUP BY 
        users.Id, users.DisplayName
) RankedUsers
WHERE 
    Rank <= 5;
