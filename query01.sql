-- Retrieve detailed post information with aggregated comment and vote counts
SELECT 
    posts.Id AS PostId,
    posts.CreationDate,
    users.DisplayName AS OwnerDisplayName,
    users.Reputation AS OwnerReputation,
    COUNT(comments.Id) AS CommentCount,
    COUNT(votes.Id) AS VoteCount,
    posts.Tags,
    posts.Score,
    EXTRACT(DAY FROM NOW() - posts.CreationDate) AS PostAgeInDays
FROM 
    posts
JOIN 
    users ON posts.OwnerUserId = users.Id
LEFT JOIN 
    comments ON comments.PostId = posts.Id
LEFT JOIN 
    votes ON votes.PostId = posts.Id
WHERE 
    posts.CreationDate >= (DATE '2014-09-14' - INTERVAL '7 days')
GROUP BY 
    posts.Id, users.Id
ORDER BY 
    posts.CreationDate DESC;
