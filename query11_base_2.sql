--query11base2
--UserStats as subquery
SELECT 
    u.DisplayName, 
    COALESCE(SUM(pcs.CommentCount), 0) AS TotalComments,
    COALESCE(AVG(pcs.AvgCommentScore), 0) AS AvgCommentScore
FROM 
    users u
LEFT JOIN (
    SELECT 
        p.OwnerUserId, 
        COUNT(c.Id) AS CommentCount,
        AVG(c.Score) AS AvgCommentScore
    FROM 
        posts p
    LEFT JOIN comments c ON p.Id = c.PostId
    GROUP BY 
        p.OwnerUserId
) pcs ON u.Id = pcs.OwnerUserId
GROUP BY 
    u.DisplayName
ORDER BY 
    TotalComments DESC;
