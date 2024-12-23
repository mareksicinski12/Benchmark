-- Retrieves the basic details of posts created in the last 7 days

SELECT 
    posts.Id AS PostId,
    posts.CreationDate,
    users.DisplayName AS OwnerDisplayName
FROM 
    posts
JOIN 
    users ON posts.OwnerUserId = users.Id
WHERE 
    posts.CreationDate >= NOW() - INTERVAL '7 days';
