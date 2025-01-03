# Benchmark
## Test Plan

| Test Case | Test Description | SQL Feature(s) Involved         | Expected Outcome                                                                 |
|-----------|------------------|---------------------------------|---------------------------------------------------------------------------------|
| 1         | Retrieve all posts along with their owner's display name, filtering posts created within the last 7 days. | JOIN, Time Functions                     | Posts with owner details filtered by recent creation dates.                     |
| 2         | Find the top 5 users with the highest average score on their posts. | Window Functions, Joins                  | A ranked list of users by average post scores, limited to 5 users.              |
| 3         | Count the number of badges for each user and categorize users by badge count using a CTE. | CTE, Aggregation                         | Badge counts categorized and grouped by user, displayed with descriptive categories. |
| 4         | Analyze post activity by hour of day (grouped by creation hour), including total posts and average score. | Time Functions, GROUPING SETS            | Aggregated post counts and scores grouped by hours of the day.                  |
| 5         | Display all posts with JSON-formatted tags and their respective tag counts. | JSON, Subqueries                         | Posts displayed with tags in JSON format along with a count of how many tags each post has. |
| 6         | Show users with missing profile images (NULL in ProfileImageUrl) and join with posts, including all posts. | FULL OUTER JOIN, Filtering, BYTEA                     | List of users with NULL ProfileImageUrl, joined with all their posts (or NULL where posts don't exist).                            |
| 7         | Validate filtering of users whose ProfileImageUrl is not NULL, does not start with 'http://i.stack.imgur.com/', and whose Base64-encoded URL length is < 70 | NOT LIKE operator LENGTH function ENCODE and convert_to functions (Base64 encoding)            |Output should exclude URLs starting with 'http://i.stack.imgur.com/' and only include valid URLs shorter than 70 characters when encoded in Base64. ( setting a size threshold to the lowest level)       |
| -8         | Identify the posts with the highest view count for each tag. | Subqueries, Joins                        | Posts with the highest view count for each tag displayed.                       |
| -9         | Calculate the total and average votes (upvotes and downvotes) for each user. | Joins, Aggregation, CTEs                 | Total and average vote counts for users, split into upvotes and downvotes.      |
| 10        | Retrieve users with more than 10 badges and display their post details if they have over 5 accepted answers. | Joins, Subqueries, CTEs (later array for titles)                        | List of qualified users with their corresponding post details.                  |
| 11        | List all badges awarded in the past 30 days and group them by day and hour of award. | Time Functions, GROUPING SETS            | Badges grouped by both day and hour, showing counts per group.                  |
| 12        | Identify users with a reputation over 10,000 and their posts with scores greater than the average score. | Joins, Window Functions, Subqueries      | Users meeting the reputation threshold with their posts above the average score.|
| 13        | Find interesting stats about the top 10 users. | Subquery, Date Functions, Joins, CTE, Window Functions | Features about users     
| 14        | -  | - | -                                                      |
| 15        | [WRONG-ROAD surprise] Build a recursive query to find all posts linked through postLinks with a depth of 5. | Recursive CTE, Joins |  explore postLinks table up to 5 levels of depth, testing recursion limits.                                                   |
| 16        | [OFF-ROAD surprise]  fetch posts where title length exceeds 50 characters using 15+ nested subqueries. |  Deeply Nested Subqueries | Tests the system’s handling of extremely long and deeply nested query structures.                                                    | 


