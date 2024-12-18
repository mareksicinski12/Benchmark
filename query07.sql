SELECT 
    u.Id AS UserId,
    u.DisplayName,
    u.ProfileImageUrl,
	LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) AS UrlSize
FROM 
	users u
WHERE 
	u.ProfileImageUrl IS NOT NULL 
	AND u.ProfileImageUrl NOT LIKE 'http://i.stack.imgur.com/%'
	AND LENGTH(ENCODE(convert_to(u.ProfileImageUrl, 'UTF8'), 'base64')) < 70

