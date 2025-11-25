
WITH max_laps AS (
    SELECT
        session_key,
        lap_number AS max_lap,
        ROW_NUMBER() OVER (
            PARTITION BY session_key
            ORDER BY lap_number DESC
        ) AS rn
    FROM {{ source('f1_data', 'raw_laps') }}
)

SELECT
    s.*,
    ml.max_lap
FROM {{ source('f1_data', 'raw_sessions') }} s
LEFT JOIN max_laps ml
    ON s.session_key = ml.session_key
    AND ml.rn = 1