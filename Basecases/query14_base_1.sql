--query14 base1
SELECT 
    p1.Id AS PostId, 
    p1.Title, 
    p1.Body, 
    u.DisplayName AS OwnerName 
FROM posts p1
LEFT JOIN users u ON p1.OwnerUserId = u.Id
WHERE p1.ParentId IS NULL
UNION ALL
SELECT 
    p2.Id AS PostId, 
    p2.Title, 
    p2.Body, 
    u.DisplayName AS OwnerName 
FROM posts p2
LEFT JOIN users u ON p2.OwnerUserId = u.Id
WHERE p2.ParentId IS NOT NULL;
