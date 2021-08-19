WITH session_features_by_user AS (
  SELECT
    user_id,
    count(*) AS number_of_actions_taken,
    count(DISTINCT action_type) AS number_of_unique_actions,
    round(avg(secs_elapsed)) AS avg_session_time_seconds,
    round(max(secs_elapsed)) AS max_session_time_seconds,
    round(min(secs_elapsed)) AS min_session_time_seconds,
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
  u.id AS user_id,
  u.gender,
  u.age,
  u.language,
  u.signup_method,
  u.date_account_created,
  s.number_of_actions_taken,
  s.number_of_unique_actions,
  s.avg_session_time_seconds,
  s.min_session_time_seconds,
  s.max_session_time_seconds
FROM
  session_features_by_user s
  LEFT JOIN users u ON u.id = s.user_id
LIMIT 5000
