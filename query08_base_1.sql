--query8base1
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
ORDER BY 
	PostCount DESC
