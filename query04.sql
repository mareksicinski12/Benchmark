-- Combine most active user data with overall hourly data and compare scores
WITH HourlyActivity AS (
    SELECT
        EXTRACT(HOUR FROM CreationDate) AS Hour,
        OwnerUserId,
        COUNT(*) AS PostCount,
        ROUND(AVG(Score), 2) AS AvgUserScore
    FROM
        posts
    GROUP BY
        EXTRACT(HOUR FROM CreationDate),
        OwnerUserId
),
MostActiveUser AS (
    SELECT
        Hour,
        OwnerUserId,
        PostCount,
        AvgUserScore,
        RANK() OVER (PARTITION BY Hour ORDER BY PostCount DESC) AS Rank
    FROM
        HourlyActivity
),
PostActivityByHour AS (
    SELECT 
        EXTRACT(HOUR FROM posts.CreationDate) AS CreationHour,
        COUNT(posts.Id) AS TotalPosts,
        ROUND(AVG(posts.Score), 2) AS AvgHourScore
    FROM 
        posts
    GROUP BY 
        GROUPING SETS (
            (EXTRACT(HOUR FROM posts.CreationDate)),
            ()
        )
)
-- Compare user activity with overall hourly activity
SELECT 
    h.Hour,
    h.OwnerUserId AS MostActiveUserId,
    h.PostCount AS MostActiveUserPostCount,
    h.AvgUserScore AS MostActiveUserAvgScore,
    p.TotalPosts AS HourTotalPosts,
    p.AvgHourScore AS HourAvgScore,
    CASE
        WHEN h.AvgUserScore > p.AvgHourScore THEN 'Better'
        WHEN h.AvgUserScore = p.AvgHourScore THEN 'Equal'
        ELSE 'Worse'
    END AS AvgScoreComparison
FROM 
    MostActiveUser h
JOIN 
    PostActivityByHour p
ON 
    h.Hour = p.CreationHour
WHERE 
    h.Rank = 1
ORDER BY 
    h.Hour;
