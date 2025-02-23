WITH UserPostSummary AS (
    SELECT 
        u.Id AS UserId, 
        u.DisplayName, 
        STRING_AGG(p.Title, '; ') AS RecentPostTitles,
        COUNT(p.Id) AS TotalPosts,
        MAX(p.CreationDate) AS LastPostDate
    FROM 
        users u
    LEFT JOIN posts p ON u.Id = p.OwnerUserId
    WHERE 
        p.CreationDate >= DATE '2014-09-01' - INTERVAL '6 months'
    GROUP BY 
        u.Id, u.DisplayName
)
SELECT 
    ups.DisplayName, 
    COALESCE(ups.RecentPostTitles, 'No Recent Posts') AS RecentPostTitles,
    ups.TotalPosts,
    ups.LastPostDate
FROM 
    UserPostSummary ups
ORDER BY 
    ups.TotalPosts DESC, ups.LastPostDate DESC;

