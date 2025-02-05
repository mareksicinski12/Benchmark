-- Using Subquery in the SELECT Clause
SELECT 
    posts.Id AS PostId,
    posts.CreationDate,
    (SELECT DisplayName FROM users WHERE users.Id = posts.OwnerUserId) AS OwnerDisplayName
FROM 
    posts
WHERE 
    posts.CreationDate >= DATE '2014-09-01' - INTERVAL '7 days';
