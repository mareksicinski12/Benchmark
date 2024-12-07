Query 13 

-- Base Query: Retrieve the top 10 users by reputation with their post count
SELECT 
    u.Id,                        -- User ID
    u.DisplayName,               -- User's display name
    u.Reputation,                -- User's reputation
    COUNT(p.Id) AS PostCount     -- Total number of posts per user
FROM 
    users u
LEFT JOIN 
    posts p ON u.Id = p.OwnerUserId -- Join posts to associate them with users
GROUP BY 
    u.Id, u.DisplayName, u.Reputation -- Group by user attributes
ORDER BY 
    u.Reputation DESC            -- Sort users by reputation in descending order
LIMIT 10;                        -- Limit to the top 10 users





===============================================================================================

-- Add Best Post Information: Find the highest-scoring post for each user
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COUNT(p.Id) AS PostCount,
    COALESCE(
        CONCAT('BEST POST: ', (
            SELECT 
                COALESCE(p1.Title, 'No Title') -- Use "No Title" if the post has no title
            FROM 
                posts p1
            WHERE 
                p1.OwnerUserId = u.Id         -- Filter posts by the current user
            ORDER BY 
                p1.Score DESC                 -- Order posts by score in descending order
            LIMIT 1                           -- Take the top post
        )),
        'No Title'
    ) AS BestPost                            -- Display the best post title with a prefix
FROM 
    users u
LEFT JOIN 
    posts p ON u.Id = p.OwnerUserId
GROUP BY 
    u.Id, u.DisplayName, u.Reputation
ORDER BY 
    u.Reputation DESC
LIMIT 10;


===============================================================================================

-- Add Membership Duration: Calculate how long users have been with the platform
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COUNT(p.Id) AS PostCount,
    COALESCE(
        CONCAT('BEST POST: ', (
            SELECT 
                COALESCE(p1.Title, 'No Title')
            FROM 
                posts p1
            WHERE 
                p1.OwnerUserId = u.Id
            ORDER BY 
                p1.Score DESC
            LIMIT 1
        )),
        'No Title'
    ) AS BestPost,
    CONCAT(
        u.DisplayName, 
        ' is with us for: ',
        EXTRACT(YEAR FROM AGE(NOW(), u.CreationDate))::TEXT, -- Extract years
        ' years',
        CASE
            WHEN EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate)) > 0 THEN
                CONCAT(' and ', EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate))::TEXT, ' months') -- Add months if > 0
            ELSE
                '' -- No months displayed if the value is 0
        END,
        ' already!'
    ) AS Membership                               -- Display a message about membership duration
FROM 
    users u
LEFT JOIN 
    posts p ON u.Id = p.OwnerUserId
GROUP BY 
    u.Id, u.DisplayName, u.Reputation
ORDER BY 
    u.Reputation DESC
LIMIT 10;


===============================================================================================

-- Add Upvote Percentage: Calculate the percentage of upvotes relative to total votes
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COUNT(p.Id) AS PostCount,
    COALESCE(
        CONCAT('BEST POST: ', (
            SELECT 
                COALESCE(p1.Title, 'No Title')
            FROM 
                posts p1
            WHERE 
                p1.OwnerUserId = u.Id
            ORDER BY 
                p1.Score DESC
            LIMIT 1
        )),
        'No Title'
    ) AS BestPost,
    CONCAT(
        u.DisplayName, 
        ' is with us for: ',
        EXTRACT(YEAR FROM AGE(NOW(), u.CreationDate))::TEXT, 
        ' years',
        CASE
            WHEN EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate)) > 0 THEN
                CONCAT(' and ', EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate))::TEXT, ' months')
            ELSE
                ''
        END,
        ' already!'
    ) AS Membership,
    ROUND(
        CASE 
            WHEN (u.UpVotes + u.DownVotes) > 0 THEN 
                (u.UpVotes::NUMERIC / (u.UpVotes + u.DownVotes)) * 100 -- Calculate percentage
            ELSE 
                0 -- Default to 0 if no votes
        END, 2
    ) AS UpvotePercentage                        -- Display percentage rounded to 2 decimal places
FROM 
    users u
LEFT JOIN 
    posts p ON u.Id = p.OwnerUserId
GROUP BY 
    u.Id, u.DisplayName, u.Reputation
ORDER BY 
    u.Reputation DESC
LIMIT 10;

===============================================================================================

-- CTE for post-related metrics
-- Calculates the total number of posts, cumulative score, first post date, and last post date for each user.
WITH UserMetrics AS (
    SELECT 
        p.OwnerUserId AS UserId,      -- The user who owns the post
        COUNT(p.Id) AS PostCount,     -- Total number of posts by the user
        SUM(p.Score) AS TotalScore,   -- Cumulative score of all the user's posts
        MIN(p.CreationDate) AS FirstPostDate, -- Earliest post date
        MAX(p.CreationDate) AS LastPostDate   -- Latest post date
    FROM 
        posts p
    GROUP BY 
        p.OwnerUserId                 -- Group by user to calculate metrics per user
),

-- CTE for user reputation rankings
-- Ranks users based on their reputation in descending order.
UserRankings AS (
    SELECT 
        u.Id AS UserId,               -- User ID
        u.Reputation,                 -- Reputation of the user
        RANK() OVER (ORDER BY u.Reputation DESC) AS ReputationRank -- Rank based on reputation
    FROM 
        users u
)

SELECT 
    u.Id,                             -- User ID
    u.DisplayName,                    -- User's display name
    u.Reputation,                     -- User's reputation
    COALESCE(um.PostCount, 0) AS PostCount, -- Total number of posts, default to 0 if no posts
    COALESCE(um.TotalScore, 0) AS TotalScore, -- Cumulative score of posts, default to 0 if no posts

    -- Best post for the user
    COALESCE(
        CONCAT('BEST POST: ', (
            SELECT 
                COALESCE(p1.Title, 'No Title') -- Use "No Title" if the post title is NULL
            FROM 
                posts p1
            WHERE 
                p1.OwnerUserId = u.Id         -- Filter posts by the current user
            ORDER BY 
                p1.Score DESC                 -- Order posts by score in descending order
            LIMIT 1                           -- Take the top post
        )),
        'No Title'
    ) AS BestPost,

    -- Calculate how long the user has been with the site in years and months
    CONCAT(
        u.DisplayName, 
        ' is with us for: ',
        EXTRACT(YEAR FROM AGE(NOW(), u.CreationDate))::TEXT, -- Extract years from membership duration
        ' years',
        CASE
            WHEN EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate)) > 0 THEN
                CONCAT(' and ', EXTRACT(MONTH FROM AGE(NOW(), u.CreationDate))::TEXT, ' months') -- Add months if > 0
            ELSE
                '' -- No months displayed if the value is 0
        END,
        ' already!'
    ) AS Membership,

    -- Count the number of badges for the user
    (SELECT COUNT(*) FROM badges b WHERE b.UserId = u.Id) AS BadgeCount,

    -- User's reputation rank from the UserRankings CTE
    ur.ReputationRank,

    -- Metrics from the UserMetrics CTE: first and last post dates
    COALESCE(um.FirstPostDate, NULL) AS FirstPostDate, -- First post date, NULL if no posts
    COALESCE(um.LastPostDate, NULL) AS LastPostDate,   -- Last post date, NULL if no posts

    -- Calculate the percentage of upvotes compared to total votes (upvotes + downvotes)
    ROUND(
        CASE 
            WHEN (u.UpVotes + u.DownVotes) > 0 THEN 
                (u.UpVotes::NUMERIC / (u.UpVotes + u.DownVotes)) * 100 -- Upvote percentage
            ELSE 
                0 -- Default to 0 if no votes
        END, 2
    ) AS UpvotePercentage
FROM 
    users u
-- Join with the UserMetrics CTE to get post-related metrics
LEFT JOIN 
    UserMetrics um ON u.Id = um.UserId
-- Join with the UserRankings CTE to get reputation rank
LEFT JOIN 
    UserRankings ur ON u.Id = ur.UserId
-- Sort the results by user reputation in descending order
ORDER BY 
    u.Reputation DESC
-- Limit the output to the top 10 users
LIMIT 10;
