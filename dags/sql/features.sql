/*
feature table

 - user_id
 - gender
 - age
 - language
 - signup_method
 - date_account_created
 - number_of_actions_taken
 - total_seconds_elapsed
 - avg_session_time
 - total_bookings
 */
WITH session_features_by_user AS (
  SELECT
    user_id,
    count(*) AS number_of_actions_taken,
    round(sum(secs_elapsed)) AS total_seconds_elapsed,
    round(avg(secs_elapsed)) AS avg_session_time_seconds,
    (
      SELECT
        count(*)
      FROM
        sessions s
      WHERE
        s.user_id = user_id
        AND s.action_type = 'booking_request') AS total_bookings
    FROM
      sessions
    GROUP BY
      user_id
)
SELECT
  *
FROM
  session_features_by_user
LIMIT
  5000
