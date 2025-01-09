--query8
WITH TagPostsStats AS (
	SELECT 
		UNNEST(string_to_array(Tags, ',')) AS TagName,
		COUNT(Id) AS PostCount,
		AVG(Score) AS AvgScore
	FROM 
		posts 
	WHERE 
		Tags IS NOT NULL 
	GROUP BY 
		TagName
)
SELECT 
	TagName,
	PostCount,
	AvgScore,
	JSON_BUILD_OBJECT('Tag', TagName, 'PostCount', PostCount, 'AvgScore', AvgScore) AS TagStats
FROM 
	TagPostsStats 
ORDER BY 
	PostCount DESC

