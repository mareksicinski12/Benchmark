--query15
WITH RECURSIVE UserInteractions AS (
    SELECT 
		c.UserId AS User1, p.OwnerUserId AS User2
    FROM 
		comments c
    JOIN 
		posts p ON c.PostId = p.Id
    WHERE 
		c.UserId IS NOT NULL 
		AND p.OwnerUserId IS NOT NULL
	
	
    UNION 
	
	
    SELECT 
		ui.User2 AS User1, p.OwnerUserId AS User2
    FROM 
		UserInteractions ui
    JOIN 
		posts p ON ui.User2 = p.OwnerUserId
)
SELECT  User1, User2
FROM UserInteractions
WHERE User1 = 13
GROUP BY User1, User2
