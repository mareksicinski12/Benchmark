Query 5

-- Query to retrieve posts with JSON-formatted tags and accurate tag counts
SELECT 
    posts.Id AS PostId,                                -- The unique identifier for the post
    posts.Title,                                       -- The title of the post
    posts.Tags AS OriginalTags,                       -- The original tag string from the post (for reference)
    -- Convert the extracted tags into a JSON array
    json_agg(tags.tag_name) AS TagsAsJSON,            -- Aggregate all extracted tags into a JSON array
    COUNT(tags.tag_name) AS TagCount                  -- Count the number of tags extracted
FROM 
    posts
-- Use a lateral join to extract all matches from the Tags column
LEFT JOIN 
    LATERAL (
        SELECT match[1] AS tag_name                   -- Extract each match group from the regular expression
        FROM regexp_matches(posts.Tags, '<([^<>]+)>', 'g') AS match
    ) tags ON true                                    -- Lateral join applies to each row
WHERE 
    posts.Tags IS NOT NULL                             -- Ensure we only process posts with tags
GROUP BY 
    posts.Id, posts.Title, posts.Tags                 -- Group by post details to aggregate tags
ORDER BY 
    TagCount DESC;                                     -- Sort results by tag count in descending order
