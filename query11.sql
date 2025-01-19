--find the top 3 user with the third total comments count , query11
WITH PostCommentStats AS (
    SELECT 
        p.Id AS PostId, 
        p.OwnerUserId, 
        p.Title, 
        COUNT(c.Id) AS CommentCount,
        AVG(c.Score) AS AvgCommentScore
    FROM 
        posts p
    FULL OUTER JOIN comments c ON p.Id = c.PostId
    GROUP BY 
        p.Id, p.OwnerUserId, p.Title
),
UserStats AS (
    SELECT 
        u.DisplayName, 
        SUM(COALESCE(pcs.CommentCount, 0)) AS TotalComments,
        AVG(COALESCE(pcs.AvgCommentScore, 0)) AS AvgCommentScore
    FROM 
        users u
    LEFT JOIN PostCommentStats pcs ON u.Id = pcs.OwnerUserId
    GROUP BY 
        u.DisplayName
)
SELECT 
    DisplayName, 
    TotalComments, 
    AvgCommentScore
FROM 
    UserStats
ORDER BY 
    TotalComments DESC, AvgCommentScore DESC

	
