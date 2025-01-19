--query11base1
WITH PostCommentStats AS (
	SELECT
		p.Id,
		p.OwnerUserId,
		p.Title,
		COUNT(c.Id) AS CommentCount,
		AVG(c.Score) AS AvgCommentScore
	FROM 
		comments c 
		LEFT OUTER JOIN posts p ON c.PostId = p.Id
	GROUP BY 
		p.Id, p.OwnerUserId, p.Title 
)
SELECT *
FROM users u
LEFT OUTER JOIN PostCommentStats pcs ON u.Id = pcs.OwnerUserId
