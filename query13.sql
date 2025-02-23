WITH UserMetrics AS (
    SELECT 
        p.OwnerUserId AS UserId,
        COUNT(p.Id) AS PostCount,
        SUM(p.Score) AS TotalScore,
        MIN(p.CreationDate) AS FirstPostDate,
        MAX(p.CreationDate) AS LastPostDate
    FROM 
        posts p
    GROUP BY 
        p.OwnerUserId
),

-- CTE for user reputation rankings
UserRankings AS (
    SELECT 
        u.Id AS UserId,
        u.Reputation,
        RANK() OVER (ORDER BY u.Reputation DESC) AS ReputationRank
    FROM 
        users u
)

-- top 10 users with detailed metrics
SELECT 
    u.Id,
    u.DisplayName,
    u.Reputation,
    COALESCE(um.PostCount, 0) AS PostCount,  -- Total posts
    COALESCE(um.TotalScore, 0) AS TotalScore, -- Cumulative post score

    -- Best post title
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

    -- Membership duration in years and months
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

    -- Total badges earned
    (SELECT COUNT(*) FROM badges b WHERE b.UserId = u.Id) AS BadgeCount,

    -- Reputation rank
    ur.ReputationRank,

    -- Post date range
    COALESCE(um.FirstPostDate, NULL) AS FirstPostDate,
    COALESCE(um.LastPostDate, NULL) AS LastPostDate,

    -- Upvote percentage
    ROUND(
        CASE 
            WHEN (u.UpVotes + u.DownVotes) > 0 THEN 
                (u.UpVotes::NUMERIC / (u.UpVotes + u.DownVotes)) * 100
            ELSE 
                0
        END, 2
    ) AS UpvotePercentage
FROM 
    users u
LEFT JOIN 
    UserMetrics um ON u.Id = um.UserId
LEFT JOIN 
    UserRankings ur ON u.Id = ur.UserId
ORDER BY 
    u.Reputation DESC
LIMIT 10;
