--query8base2
-- Replace the UNNEST with a subquery to handle tags.
SELECT 
	TagName,
	COUNT(Id) AS PostCount,
	AVG(Score) AS AvgScore
FROM 
	(
		SELECT 
			UNNEST(string_to_array(Tags, ',')) AS TagName,
			Id,
			Score
		FROM posts
		WHERE Tags IS NOT NULL 
		
	) t
GROUP BY 
	TagName
ORDER BY 
	PostCount DESC