SELECT 
    u.Id AS UserId, 
    u.DisplayName, 
    p.Id AS PostId, 
    p.Title, 
    COALESCE(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64'), 'No Image') AS ProfileImageBase64
FROM users u
FULL OUTER JOIN posts p ON u.Id = p.OwnerUserId
WHERE u.ProfileImageUrl IS NULL;
