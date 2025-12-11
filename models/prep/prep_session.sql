WITH race_positions AS (
    SELECT DISTINCT
        session_key,
        driver_number,
        FIRST_VALUE(position) OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY lap_number ASC
        ) AS race_start_pos,
        FIRST_VALUE(position) OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY lap_number DESC
        ) AS race_end_pos
    FROM {{ ref("stg_pos_laps") }} p
)
SELECT
    s.*,
    d.full_name,
    d.driver_number,
    d.headshot_url,
    rp.race_start_pos,
    rp.race_end_pos,
    (rp.race_start_pos - rp.race_end_pos) AS race_pos_dif
FROM {{ ref("stg_session") }} s
JOIN race_positions rp USING (session_key)
JOIN {{ ref("stg_drivers") }} d USING (session_key, driver_number)
ORDER BY s.session_key, d.driver_number
