Query 3

-- Use a CTE to count the badges for each user.
WITH BadgeCounts AS (
    SELECT 
        users.Id AS UserId,                    -- The unique ID of the user
        users.DisplayName AS UserName,         -- The display name of the user
        COUNT(badges.Id) AS BadgeCount         -- Total number of badges the user has earned
    FROM 
        users
    LEFT JOIN 
        badges ON users.Id = badges.UserId     -- Left join to include users even if they have no badges
    GROUP BY 
        users.Id, users.DisplayName            -- Group by user ID and name to calculate badge counts
)

-- Categorize users based on their badge counts.
SELECT 
    UserId,                                   -- User ID from the CTE
    UserName,                                 -- User display name from the CTE
    BadgeCount,                               -- Total badges earned
    CASE 
        WHEN BadgeCount = 0 THEN 'No Badges'          -- Users with zero badges
        WHEN BadgeCount BETWEEN 1 AND 5 THEN 'Bronze' -- Users with 1 to 5 badges
        WHEN BadgeCount BETWEEN 6 AND 15 THEN 'Silver' -- Users with 6 to 15 badges
        ELSE 'Gold'                                   -- Users with more than 15 badges
    END AS BadgeCategory                      -- Categorization of users based on badge count
FROM 
    BadgeCounts                               -- Referencing the CTE
ORDER BY 
    BadgeCount DESC;                          -- Sorting by badge count in descending order
