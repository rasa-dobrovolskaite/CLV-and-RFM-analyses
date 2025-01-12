```sql
WITH cohorts AS (
SELECT
  user_pseudo_id AS user_id,
  purchase_revenue_in_usd AS revenue,
  DATE_TRUNC(PARSE_DATE("%Y%m%d", event_date), WEEK) AS event_cohort,
  MIN(DATE_TRUNC(PARSE_DATE("%Y%m%d", event_date), WEEK)) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS registration_cohort
FROM
  `turing_data_analytics.raw_events`
WHERE
  PARSE_DATE("%Y%m%d", event_date) < "2021-01-31"
)
-- This CTE is used to identify registration and weekly cohorts. 

SELECT
  registration_cohort,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 0 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_0,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 1 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_1,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 2 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_2,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 3 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_3,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 4 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_4,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 5 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_5,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 6 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_6,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 7 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_7,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 8 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_8,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 9 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_9,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 10 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_10,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 11 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_11,
  SUM(CASE WHEN DATE_DIFF(event_cohort, registration_cohort, WEEK) = 12 THEN revenue ELSE 0 END) / COUNT(DISTINCT user_id) AS week_12,
  -- Calculations are done separately for each week.
  -- DATE_DIFF function is used to determine the week that each event falls into. 
  -- If event falls into specific week, revenue is calculated by summing up revenue that happened each week.
  -- Lastly, revenue is divided by registrations (number of users).
FROM
  cohorts
GROUP BY
  registration_cohort
ORDER BY
  registration_cohort
