-- Using Window Functions
WITH RankedUsers AS (
    SELECT 
        u.Id,
        u.DisplayName,
        u.Reputation,
        COUNT(p.Id) AS PostCount,
        ROW_NUMBER() OVER (ORDER BY u.Reputation DESC) AS RowNum
    FROM 
        users u
    LEFT JOIN 
        posts p ON u.Id = p.OwnerUserId
    GROUP BY 
        u.Id, u.DisplayName, u.Reputation
)
SELECT 
    Id,
    DisplayName,
    Reputation,
    PostCount
FROM 
    RankedUsers
WHERE 
    RowNum <= 10;
