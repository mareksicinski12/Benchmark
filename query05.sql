-- Retrieve posts with JSON-formatted tags and calculate tag counts
SELECT 
    posts.Id AS PostId,
    posts.Title,
    posts.Tags AS OriginalTags,
    json_agg(tags.tag_name) AS TagsAsJSON, -- Aggregate tags into a JSON array
    COUNT(tags.tag_name) AS TagCount      -- Count the number of extracted tags
FROM 
    posts
-- Extract tags using a lateral join with regex
LEFT JOIN 
    LATERAL (
        SELECT match[1] AS tag_name
        FROM regexp_matches(posts.Tags, '<([^<>]+)>', 'g') AS match
    ) tags ON true
-- Process only posts that have tags
WHERE 
    posts.Tags IS NOT NULL
GROUP BY 
    posts.Id, posts.Title, posts.Tags
ORDER BY 
    TagCount DESC;
