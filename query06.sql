WITH AvgPostScore AS (
    SELECT 
        AVG(p.Score) AS AverageScore
    FROM 
        posts p
)
SELECT 
    u.Id AS UserId,
    u.DisplayName AS UserDisplayName,
    p.Id AS PostId,
    p.Title AS PostTitle,
    p.Score AS PostScore,
    COALESCE(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64'), 'No Image') AS ProfileImageBase64
FROM 
    users u
    FULL OUTER JOIN posts p ON u.Id = p.OwnerUserId
    JOIN AvgPostScore aps ON p.Score > aps.AverageScore
WHERE 
    u.Reputation > 10000  
    AND p.Title IS NOT NULL
    AND u.ProfileImageUrl IS NULL
ORDER BY 
    u.Reputation DESC, p.Score DESC;
