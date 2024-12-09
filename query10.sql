--comparing to base - query retrieves top 3 posts of users with the most views
--put these post titles into an array showing the date of last post added 
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
	ORDER BY 
    	u.Reputation DESC, 
		p.CreationDate DESC
),
RankedUserPosts AS (
	SELECT
		UserId,
		DisplayName,
		NumberOfBadges,
		Reputation,
		PostTitle,
		ViewCount,
		CreationDate,
		ROW_NUMBER() OVER (PARTITION BY UserId ORDER BY ViewCount DESC) AS Rank
	FROM 
		UserPosts
),
UserPostsGrouped AS (
	SELECT 
		UserId,
		DisplayName,
		NumberOfBadges,
		Reputation,
		ARRAY_AGG(PostTitle ORDER BY Rank) AS PostTitles,
		MAX(CreationDate) AS LastPostCreationTime
	FROM 
		RankedUserPosts
	WHERE 
		Rank <= 3 
	GROUP BY 
		UserId, DisplayName, NumberOfBadges, Reputation
	ORDER BY 
		Reputation DESC
)
SELECT *
FROM UserPostsGrouped
