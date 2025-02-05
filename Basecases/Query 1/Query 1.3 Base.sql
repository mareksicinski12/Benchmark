-- CTE
WITH RecentPosts AS (
    SELECT 
        Id AS PostId, 
        CreationDate, 
        OwnerUserId
    FROM 
        posts
    WHERE 
        CreationDate >= DATE '2014-09-01' - INTERVAL '7 days'
)
SELECT 
    RecentPosts.PostId,
    RecentPosts.CreationDate,
    users.DisplayName AS OwnerDisplayName
FROM 
    RecentPosts
JOIN 
    users ON RecentPosts.OwnerUserId = users.Id;
