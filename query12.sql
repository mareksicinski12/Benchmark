WITH AvgPostScore AS (
    SELECT 
        AVG(p.Score) AS AverageScore
    FROM 
        posts p
),
UserPostsWithRank AS (
	SELECT 
    u.Id AS UserId,
    u.DisplayName AS UserDisplayName,
    p.Id AS PostId,
    p.Title AS PostTitle,
    p.Score AS PostScore,
	RANK() OVER (PARTITION BY u.Id ORDER BY p.Score DESC) AS PostRank
FROM 
    users u
	JOIN posts p ON u.Id = p.OwnerUserId
	JOIN AvgPostScore aps ON p.Score > aps.AverageScore
WHERE 
    u.Reputation > 10000 
	AND p.Title IS NOT NULL
),
UserPostsGrouped AS (
	SELECT 
		UserId,
		CASE 
			WHEN PostScore > 45 THEN 'high score'
			WHEN PostScore BETWEEN 20 AND 45 THEN 'medium score'
			ELSE 'low score'
		END AS ScoreCategory,
		COUNT(*) AS PostCount,
		ARRAY_AGG(PostTitle) FILTER (WHERE PostTitle IS NOT NULL) AS PostTitles
	FROM 
		UserPostsWithRank
	GROUP BY 
		GROUPING SETS(
			(UserId), 
			(UserId,ScoreCategory)
			 )
		
)
SELECT 
	u.DisplayName AS UserDisplayName,
	upg.ScoreCategory,
	upg.PostCount,
	--MAX(upr.PostRank) AS LowestPostRank,
	upg.PostTitles
FROM 
	UserPostsGrouped upg
	JOIN users u ON u.Id = upg.UserId
	JOIN UserPostsWithRank upr ON upr.UserId = upg.UserId
WHERE
	upg.ScoreCategory IS NOT NULL 
    AND upg.PostTitles IS NOT NULL  
    AND upg.PostCount > 0           
    --AND upr.PostRank IS NOT NULL 
GROUP BY 
    u.DisplayName, upg.ScoreCategory, upg.PostCount, upg.PostTitles
ORDER BY 
    u.DisplayName, upg.ScoreCategory DESC
	
	

