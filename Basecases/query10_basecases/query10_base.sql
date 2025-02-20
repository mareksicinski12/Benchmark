WITH BadgeCount AS (
	SELECT
		b.UserId,
		COUNT(b.Id) AS NumberOfBadges
	FROM
		badges b 
	GROUP BY 
		b.UserId
),
UserAcceptedAnswers AS (
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
),
UserPosts AS (
	SELECT
		u.Id AS UserId,
		u.DisplayName,
		bc.NumberOfBadges,
		p.Title AS PostTitle,
		u.Reputation,
		p.ViewCount,
		p.CreationDate
	FROM 
		users u
		JOIN BadgeCount bc ON u.Id = bc.UserId
		JOIN UserAcceptedAnswers ua ON u.Id = ua.OwnerUserId
		JOIN posts p ON u.Id = p.OwnerUserId
	WHERE 
		bc.NumberOfBadges > 50 
		AND ua.AcceptedAnswerCount > 5
		AND p.Title IS NOT NULL
)
SELECT *
FROM UserPosts
ORDER BY Reputation DESC, CreationDate DESC;
