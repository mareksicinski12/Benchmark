--the query 9 without the STRING_AGG function.
WITH UserPostSummary AS (
    SELECT 
        u.Id AS UserId, 
        u.DisplayName, 
		p.Title,
        COUNT(p.Id) AS TotalPosts,
        MAX(p.CreationDate) AS LastPostDate
    FROM 
        users u
    LEFT JOIN posts p ON u.Id = p.OwnerUserId
    WHERE 
        p.CreationDate >= DATE '2014-09-01' - INTERVAL '6 months'
    GROUP BY 
        u.Id, u.DisplayName, p.Title
)
SELECT 
    DisplayName, 
	Title,
    TotalPosts, 
    LastPostDate
FROM 
    UserPostSummary
WHERE 
	Title IS NOT NULL
ORDER BY 
    TotalPosts DESC
