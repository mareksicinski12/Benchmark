--query14, 
--Retrieve All Posts and Their Recursive Parent-Child Relationships with User Details
--Uses recursive query to retrieve parent-child relationships of posts.
--Applies a text function to limit post bodies to 100 characters.
--Combines with a FULL OUTER JOIN to include posts without users and users without posts.
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
    JOIN PostHierarchy ph ON p.ParentId = ph.PostId
)
SELECT 
    ph.PostId, 
    ph.Title, 
    LEFT(ph.Body, 100) AS BodySnippet, 
    u.DisplayName AS OwnerName 
FROM PostHierarchy ph
FULL OUTER JOIN users u ON u.Id = ph.PostId
ORDER BY ph.PostId

