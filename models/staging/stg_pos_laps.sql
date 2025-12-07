WITH lap_timing AS (
    SELECT
        meeting_key,
        session_key,
        driver_number,
        lap_number,
        COALESCE(
            LEAD(date_start) OVER (PARTITION BY meeting_key, session_key, driver_number ORDER BY lap_number),
            (date_start + INTERVAL '1 second' * (lap_duration + 5))
        ) AS effective_lap_end
    FROM {{ source('f1_data', 'raw_laps') }}
),
all_ranked AS (
    SELECT
        l.meeting_key,
        l.session_key,
        l.driver_number,
        l.lap_number,
        p.position,
        ROW_NUMBER() OVER (
            PARTITION BY l.meeting_key, l.session_key, l.driver_number, l.lap_number 
            ORDER BY p.date DESC
        ) AS rn
    FROM lap_timing l
    INNER JOIN {{ source('f1_data', 'raw_position') }} p
        ON l.meeting_key = p.meeting_key 
        AND l.session_key = p.session_key 
        AND l.driver_number = p.driver_number
        AND p.date <= l.effective_lap_end

    UNION ALL

    SELECT
        meeting_key,
        session_key,
        driver_number,
        0 AS lap_number,
        position,
        ROW_NUMBER() OVER (
            PARTITION BY meeting_key, session_key, driver_number 
            ORDER BY date ASC
        ) AS rn
    FROM {{ source('f1_data', 'raw_position') }}
)
SELECT
    meeting_key,
    session_key,
    driver_number,
    lap_number,
    position
FROM all_ranked
WHERE rn = 1
ORDER BY meeting_key, session_key, driver_number, lap_number