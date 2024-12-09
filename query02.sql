-- Retrieve top-ranked users based on the average score of their posts within the last year
SELECT 
    users.Id AS UserId,
    users.DisplayName AS UserName,
    users.Reputation AS UserReputation,
    COUNT(posts.Id) AS TotalPosts,
    AVG(posts.Score) AS AverageScore,
    RANK() OVER (ORDER BY AVG(posts.Score) DESC) AS Rank
FROM 
    users
JOIN 
    posts ON users.Id = posts.OwnerUserId
WHERE 
    posts.CreationDate >= (DATE '2014-09-14' - INTERVAL '1 year') -- Posts within the last year
GROUP BY 
    users.Id, users.DisplayName, users.Reputation
HAVING 
    COUNT(posts.Id) > 10 -- Only include users with more than 10 posts
ORDER BY 
    Rank ASC;
