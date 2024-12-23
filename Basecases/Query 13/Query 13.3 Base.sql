-- Using Subquery to Pre-Aggregate Post Counts
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COALESCE(PostCounts.PostCount, 0) AS PostCount
FROM 
    users u
LEFT JOIN (
    SELECT 
        p.OwnerUserId,
        COUNT(p.Id) AS PostCount
    FROM 
        posts p
    GROUP BY 
        p.OwnerUserId
) PostCounts ON u.Id = PostCounts.OwnerUserId
ORDER BY 
    u.Reputation DESC
LIMIT 10;
