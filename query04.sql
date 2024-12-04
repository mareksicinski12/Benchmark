Query 4

-- This query analyzes post activity by hour of day.
-- It calculates the total posts and average score for each creation hour,
-- and includes subtotals (for all hours combined) and grand totals using GROUPING SETS.
WITH PostActivityByHour AS (
    SELECT 
        EXTRACT(HOUR FROM posts.CreationDate) AS CreationHour, -- Extracts the hour from the post's creation timestamp
        COUNT(posts.Id) AS TotalPosts,                        -- Total number of posts created in this hour
        ROUND(AVG(posts.Score), 2) AS AverageScore            -- Average score of posts created in this hour (rounded to 2 decimals)
    FROM 
        posts
    GROUP BY 
        GROUPING SETS (
            (EXTRACT(HOUR FROM posts.CreationDate)),          -- Group by each individual hour of the day
            ()                                                -- Include a grand total (all hours combined)
        )
)
SELECT 
    CreationHour,           -- The hour of post creation (NULL for grand total)
    TotalPosts,             -- Total posts created in this hour
    AverageScore,           -- Average score of posts created in this hour
    CASE 
        WHEN CreationHour IS NULL THEN 1 
        ELSE 0 
    END AS IsTotal          -- Marks rows as grand total (1) or not (0)
FROM 
    PostActivityByHour
ORDER BY 
    CASE WHEN CreationHour IS NULL THEN 1 ELSE 0 END, -- Ensures the grand total appears last
    CreationHour;


======================================================================================================================

-- Calculate post counts and average scores for each user grouped by hour
WITH HourlyActivity AS (
    SELECT
        EXTRACT(HOUR FROM CreationDate) AS Hour,  -- Extract the hour from the post creation timestamp
        OwnerUserId,                              -- The user who owns the post
        COUNT(*) AS PostCount,                    -- Total number of posts made by the user in the hour
        ROUND(AVG(Score), 2) AS AvgUserScore      -- Average score of the user's posts, rounded to 2 decimals
    FROM
        posts
    GROUP BY
        EXTRACT(HOUR FROM CreationDate),         -- Group by the extracted hour
        OwnerUserId                              -- Group by the user ID
),
-- Rank users within each hour based on the number of posts they made
MostActiveUser AS (
    SELECT
        Hour,                                    -- Hour of the day
        OwnerUserId,                             -- The user being evaluated
        PostCount,                               -- Number of posts made by the user in that hour
        AvgUserScore,                            -- The user's average post score for the hour
        RANK() OVER (PARTITION BY Hour ORDER BY PostCount DESC) AS Rank -- Rank users by post count (highest first)
    FROM
        HourlyActivity
)
-- Retrieve the most active user (Rank = 1) for each hour
SELECT
    Hour,                                        -- Hour of the day
    OwnerUserId,                                 -- Most active user's ID
    PostCount,                                   -- Number of posts made by the most active user
    AvgUserScore                                 -- Average score of the most active user's posts
FROM
    MostActiveUser
WHERE
    Rank = 1                                     -- Filter to include only the most active user for each hour
ORDER BY
    Hour;                                        -- Sort results by hour


==================================================================================================================

-- Calculate post counts and average scores for each user grouped by hour
WITH HourlyActivity AS (
    SELECT
        EXTRACT(HOUR FROM CreationDate) AS Hour,  -- Extract the hour from the post creation timestamp
        OwnerUserId,                              -- The user who owns the post
        COUNT(*) AS PostCount,                    -- Total number of posts made by the user in the hour
        ROUND(AVG(Score), 2) AS AvgUserScore      -- Average score of the user's posts, rounded to 2 decimals
    FROM
        posts
    GROUP BY
        EXTRACT(HOUR FROM CreationDate),         -- Group by the extracted hour
        OwnerUserId                              -- Group by the user ID
),
-- Rank users within each hour based on the number of posts they made
MostActiveUser AS (
    SELECT
        Hour,                                    -- Hour of the day
        OwnerUserId,                             -- The user being evaluated
        PostCount,                               -- Number of posts made by the user in that hour
        AvgUserScore,                            -- The user's average post score for the hour
        RANK() OVER (PARTITION BY Hour ORDER BY PostCount DESC) AS Rank -- Rank users by post count (highest first)
    FROM
        HourlyActivity
),
-- Calculate total posts and average score for each hour (and all hours combined)
PostActivityByHour AS (
    SELECT 
        EXTRACT(HOUR FROM posts.CreationDate) AS CreationHour, -- Extract the hour of the post creation timestamp
        COUNT(posts.Id) AS TotalPosts,                        -- Total number of posts in that hour
        ROUND(AVG(posts.Score), 2) AS AvgHourScore            -- Average score of all posts in that hour
    FROM 
        posts
    GROUP BY 
        GROUPING SETS (
            (EXTRACT(HOUR FROM posts.CreationDate)),          -- Group by each hour individually
            ()                                                -- Include a grand total for all hours combined
        )
)
-- Combine most active user data with overall hourly data and compare average scores
SELECT 
    h.Hour,                                       -- Hour of the day
    h.OwnerUserId AS MostActiveUserId,           -- Most active user's ID
    h.PostCount AS MostActiveUserPostCount,      -- Number of posts made by the most active user
    h.AvgUserScore AS MostActiveUserAvgScore,    -- Average score of the most active user's posts
    p.TotalPosts AS HourTotalPosts,              -- Total number of posts in the hour
    p.AvgHourScore AS HourAvgScore,              -- Average score of all posts in the hour
    CASE
        WHEN h.AvgUserScore > p.AvgHourScore THEN 'Better' -- Compare user and overall scores
        WHEN h.AvgUserScore = p.AvgHourScore THEN 'Equal'
        ELSE 'Worse'
    END AS AvgScoreComparison                    -- Indicates whether the user's score is better, equal, or worse
FROM 
    MostActiveUser h                             -- Data about the most active user
JOIN 
    PostActivityByHour p                         -- Data about overall hourly activity
ON 
    h.Hour = p.CreationHour                      -- Match the hour between the two datasets
WHERE 
    h.Rank = 1                                   -- Only include the most active user per hour
ORDER BY 
    h.Hour;                                      -- Sort results by hour
