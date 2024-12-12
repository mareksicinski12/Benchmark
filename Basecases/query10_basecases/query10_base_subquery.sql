--EXPLAIN ANALYZE
SELECT
	u.Id,
	u.DisplayName,
	bc.NumberOfBadges,
	p.Title,
	p.CreationDate
FROM 
	users u	
	JOIN (
		SELECT
			b.UserId,
			COUNT(b.Id) AS NumberOfBadges
		FROM
			badges b 
		GROUP BY 
			b.UserId
	) bc ON u.Id = bc.UserId
	JOIN (
		SELECT 
        p.OwnerUserId,
        COUNT(p.Id) AS AcceptedAnswerCount
    	FROM 
        posts p
    	WHERE 
        	p.PostTypeId = 2 
        	AND p.Id IN (
            	SELECT AcceptedAnswerId 
            	FROM posts
            	WHERE AcceptedAnswerId IS NOT NULL
        	) 
    	GROUP BY 
        	p.OwnerUserId
	) ua ON u.Id = ua.OwnerUserId
	JOIN 
		posts p ON u.Id = p.OwnerUserId
WHERE 
	bc.NumberOfBadges > 10 
	AND ua.AcceptedAnswerCount > 5
	AND p.Title IS NOT NULL
ORDER BY 
    u.Reputation DESC, 
	  p.CreationDate DESC
