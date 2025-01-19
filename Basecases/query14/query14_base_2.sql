--base2 query14
WITH RECURSIVE PostHierarchy AS (
    SELECT 
        Id AS PostId, 
        ParentId, 
        Title, 
        Body 
    FROM posts
    WHERE ParentId IS NULL
    UNION ALL
    SELECT 
        p.Id AS PostId, 
        p.ParentId, 
        p.Title, 
        p.Body 
    FROM posts p
    INNER JOIN PostHierarchy ph ON p.ParentId = ph.PostId
)
SELECT 
    ph.PostId, 
    ph.Title, 
    ph.Body, 
    u.DisplayName AS OwnerName 
FROM PostHierarchy ph
LEFT JOIN users u ON ph.PostId = u.Id;
