SELECT 
    u.Id AS UserId,
    u.DisplayName,
    COUNT(b.Id) AS NumberOfBadges,
    u.Reputation,
    p.Title AS PostTitle,
    p.ViewCount
FROM users u
JOIN badges b ON u.Id = b.UserId
JOIN posts p ON u.Id = p.OwnerUserId
WHERE p.Title IS NOT NULL
GROUP BY u.Id, u.DisplayName, u.Reputation, p.Title, p.ViewCount
HAVING COUNT(b.Id) > 50
ORDER BY u.Reputation DESC;
