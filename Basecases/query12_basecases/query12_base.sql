SELECT 
    u.Id AS UserId,
    u.DisplayName AS UserDisplayName,
    p.Title AS PostTitle,
    p.Score AS PostScore,
    CASE 
        WHEN p.Score > 45 THEN 'high score'
        WHEN p.Score BETWEEN 20 AND 45 THEN 'medium score'
        ELSE 'low score'
    END AS ScoreCategory
FROM users u
JOIN posts p ON u.Id = p.OwnerUserId
WHERE 
    u.Reputation > 10000 
    AND p.Title IS NOT NULL;
