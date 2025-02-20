SELECT 
    u.Id AS UserId,
    u.DisplayName,
    COUNT(b.Id) AS NumberOfBadges,
    u.Reputation,
    COUNT(DISTINCT p2.Id) AS AcceptedAnswerCount,
    p.Title AS PostTitle,
    p.ViewCount,
    p.CreationDate
FROM users u
JOIN badges b ON u.Id = b.UserId
JOIN posts p ON u.Id = p.OwnerUserId
LEFT JOIN posts p2 ON p2.OwnerUserId = u.Id 
    AND p2.PostTypeId = 2 
    AND p2.Id IN (SELECT AcceptedAnswerId FROM posts WHERE AcceptedAnswerId IS NOT NULL)
WHERE p.Title IS NOT NULL
GROUP BY u.Id, u.DisplayName, u.Reputation, p.Title, p.ViewCount, p.CreationDate
HAVING COUNT(b.Id) > 50 AND COUNT(DISTINCT p2.Id) > 5
ORDER BY u.Reputation DESC, p.CreationDate DESC;
