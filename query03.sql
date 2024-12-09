-- Use a CTE to count the badges for each user
WITH BadgeCounts AS (
    SELECT 
        users.Id AS UserId,
        users.DisplayName AS UserName,
        COUNT(badges.Id) AS BadgeCount
    FROM 
        users
    LEFT JOIN 
        badges ON users.Id = badges.UserId
    GROUP BY 
        users.Id, users.DisplayName
)

-- Categorize users based on their badge counts
SELECT 
    UserId,
    UserName,
    BadgeCount,
    CASE 
        WHEN BadgeCount = 0 THEN 'No Badges'
        WHEN BadgeCount BETWEEN 1 AND 5 THEN 'Bronze'
        WHEN BadgeCount BETWEEN 6 AND 15 THEN 'Silver'
        ELSE 'Gold'
    END AS BadgeCategory
FROM 
    BadgeCounts
ORDER BY 
    BadgeCount DESC;
