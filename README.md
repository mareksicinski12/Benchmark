# Benchmark
## Test Plan

| Test Case | Test Description | SQL Feature(s) Involved         | Expected Outcome                                                                 |
|-----------|------------------|---------------------------------|---------------------------------------------------------------------------------|
| 1         | Retrieve all posts along with their owner's display name, filtering posts created within the last 7 days. | JOIN, Time Functions                     | Posts with owner details filtered by recent creation dates.                     |
| 2         | Find the top 5 users with the highest average score on their posts. | Window Functions, Joins                  | A ranked list of users by average post scores, limited to 5 users.              |
| 3         | Count the number of badges for each user and categorize users by badge count using a CTE. | CTE, Aggregation                         | Badge counts categorized and grouped by user, displayed with descriptive categories. |
| 4         | Analyze post activity by hour of day (grouped by creation hour), including total posts and average score. | Time Functions, GROUPING SETS            | Aggregated post counts and scores grouped by hours of the day.                  |
| 5         | Display all posts with JSON-formatted tags and their respective tag counts. | JSON, Subqueries                         | Posts displayed with tags in JSON format along with a count of how many tags each post has. |
| -6         | Show the total number of votes for each post type (answer, question), categorized by vote type. | GROUPING SETS, Joins                     | Aggregated vote counts per post type and vote type.                             |
| -7         | List the top 3 most-used tags, along with the total number of posts and average score per tag. | Window Functions, Aggregation            | Tags ranked by usage, showing post count and average score for the top 3.       |
| -8         | Identify the posts with the highest view count for each tag. | Subqueries, Joins                        | Posts with the highest view count for each tag displayed.                       |
| -9         | Calculate the total and average votes (upvotes and downvotes) for each user. | Joins, Aggregation, CTEs                 | Total and average vote counts for users, split into upvotes and downvotes.      |
| 10        | Retrieve users with more than 10 badges and display their post details if they have over 5 accepted answers. | Joins, Subqueries                        | List of qualified users with their corresponding post details.                  |
| 11        | List all badges awarded in the past 30 days and group them by day and hour of award. | Time Functions, GROUPING SETS            | Badges grouped by both day and hour, showing counts per group.                  |
| 12        | Identify users with a reputation over 10,000 and their posts with scores greater than the average score. | Joins, Window Functions, Subqueries      | Users meeting the reputation threshold with their posts above the average score.|
| 13        | Find interesting stats about the top 10 users. | Subquery, Date Functions, Joins, CTE, Window Functions | Features about users                                                          |
