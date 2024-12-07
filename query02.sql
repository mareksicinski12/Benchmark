Query 2

-- This query finds the top 5 users with the highest average score on their posts.
SELECT 
    users.Id AS UserId,                 -- Select the unique identifier for the user
    users.DisplayName AS UserName,     -- Select the display name of the user
    AVG(posts.Score) AS AverageScore   -- Calculate the average score of the user's posts
FROM 
    users                              -- From the 'users' table
JOIN 
    posts ON users.Id = posts.OwnerUserId -- Join the 'posts' table on the condition that the user's ID matches the owner of the post
GROUP BY 
    users.Id,                          -- Group the results by the user's ID to aggregate scores per user
    users.DisplayName                  -- Also group by the user's display name to ensure a consistent grouping
ORDER BY 
    AverageScore DESC                  -- Sort the results by the average score in descending order (highest scores first)
LIMIT 5;                               -- Limit the result to the top 5 users with the highest average scores


==============================================================

SELECT 
    users.Id AS UserId,                    -- The unique ID of the user
    users.DisplayName AS UserName,         -- The display name of the user
    users.Reputation AS UserReputation,    -- The reputation of the user
    COUNT(posts.Id) AS TotalPosts,         -- The total number of posts created by the user
    AVG(posts.Score) AS AverageScore,      -- The average score of the user's posts
    RANK() OVER (ORDER BY AVG(posts.Score) DESC) AS Rank -- Dynamic ranking based on average score
FROM 
    users
JOIN 
    posts ON users.Id = posts.OwnerUserId  -- Joining posts with users to link posts to their owners
WHERE 
    posts.CreationDate >= (DATE '2014-09-14' - INTERVAL '1 year') -- Filtering posts within 1 year before the given date
GROUP BY 
    users.Id, users.DisplayName, users.Reputation -- Grouping by user ID, display name, and reputation
HAVING 
    COUNT(posts.Id) > 10                  -- Excluding users with fewer than 10 posts
ORDER BY 
    Rank ASC;                             -- Sorting by the dynamic rank in ascending order
