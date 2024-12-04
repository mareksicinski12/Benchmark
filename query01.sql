Query 1

-- This query retrieves the basic details of posts created in the last 7 days
-- It fetches:
--   1. Post ID 
--   2. Post creation date 
--   3. The display name of the owner of the post

SELECT 
    posts.Id AS PostId,                  -- The unique identifier of the post
    posts.CreationDate,                  -- The timestamp when the post was created
    users.DisplayName AS OwnerDisplayName -- The display name of the post's owner
FROM 
    posts
JOIN 
    users ON posts.OwnerUserId = users.Id -- Joining the `posts` and `users` tables to associate posts with their owners
WHERE 
    posts.CreationDate >= NOW() - INTERVAL '7 days'; -- Filter posts created in the last 7 days


===============================================================

SELECT 
    posts.Id AS PostId,                  -- The unique identifier of the post
    posts.CreationDate,                  -- The timestamp when the post was created
    users.DisplayName AS OwnerDisplayName -- The display name of the post's owner
FROM 
    posts
JOIN 
    users ON posts.OwnerUserId = users.Id -- Joining the `posts` and `users` tables to associate posts with their owners
WHERE 
    posts.CreationDate >= (DATE '2014-09-14' - INTERVAL '7 days') -- Filter posts created within the 7 days before '2014-09-14'
ORDER BY 
    posts.CreationDate DESC; -- Sort results by creation date in descending order


===============================================================

-- The query retrieves the details such as:
--   1. The user's reputation
--   2. The number of comments on the post
--   3. The number of votes the post has received
--   4. Post tags
--   5. Post score
--   6. The age of the post in days
-- The query also groups data to correctly aggregate comment and vote counts for each post.

SELECT 
    posts.Id AS PostId,                        -- The unique identifier of the post
    posts.CreationDate,                        -- The timestamp when the post was created
    users.DisplayName AS OwnerDisplayName,     -- The display name of the post's owner
    users.Reputation AS OwnerReputation,       -- The reputation of the user who created the post
    COUNT(comments.Id) AS CommentCount,        -- The total number of comments associated with the post
    COUNT(votes.Id) AS VoteCount,              -- The total number of votes associated with the post
    posts.Tags,                                -- The tags associated with the post
    posts.Score,                               -- The score of the post (upvotes minus downvotes)
    EXTRACT(DAY FROM NOW() - posts.CreationDate) AS PostAgeInDays -- The age of the post in days
FROM 
    posts
JOIN 
    users ON posts.OwnerUserId = users.Id      -- Joining `posts` with `users` to retrieve owner details
LEFT JOIN 
    comments ON comments.PostId = posts.Id    -- Left join to include comment counts, even if a post has no comments
LEFT JOIN 
    votes ON votes.PostId = posts.Id          -- Left join to include vote counts, even if a post has no votes
WHERE 
    posts.CreationDate >= (DATE '2014-09-14' - INTERVAL '7 days') -- Filter posts created within the 7 days before '2014-09-14'
GROUP BY 
    posts.Id, users.Id                        -- Group by post and user IDs to aggregate comments and votes correctly
ORDER BY 
    posts.CreationDate DESC;                  -- Sort results by creation date in descending order
