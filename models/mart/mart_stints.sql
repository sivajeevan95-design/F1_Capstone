WITH base_stints AS (
    SELECT 
        meeting_key,
        session_key,
        driver_number,
        stint_number,
        compound,
        lap_start,
        lap_end,
        tyre_age_at_start,

        (lap_end - lap_start + 1) AS stint_length,

        (COUNT(*) OVER (PARTITION BY session_key, driver_number) - 1) AS total_pits
    
    FROM {{ ref('prep_stints') }}
),

stints_with_bins AS (
    SELECT 
        *,
        CASE 
            WHEN stint_length <= 12 THEN 'Short'
            WHEN stint_length <= 25 THEN 'Medium'
            ELSE 'Long'
        END AS stint_length_bin,

        CASE 
            WHEN total_pits = 0 THEN 'No-stop'
            WHEN total_pits = 1 THEN '1-stop'
            WHEN total_pits = 2 THEN '2-stop'
            ELSE '3+-stop'
        END AS pit_strategy_bin

    FROM base_stints
)

SELECT
    st.meeting_key,
    st.session_key,
    st.stint_number,
    st.driver_number,
    s.full_name as driver_name,

    st.compound,
    st.stint_length,
    st.lap_start,
    st.lap_end,
    st.tyre_age_at_start,
    st.stint_length_bin,

    st.total_pits,
    st.pit_strategy_bin,

    s.location,
    s.date_start,
    s.country_name,
    s.circuit_short_name,
    s.year,
    s.max_lap,
    s.pos_diff,

    w.avg_air_temp,
    w.safety_car,
    w.avg_track_temp,
    w.rainfall_bool,
    w.avg_humidity,
    w.avg_pressure,
    w.avg_wind_speed,

    r.dnf,
    r.dns,
    r.dsq

FROM stints_with_bins st

LEFT JOIN {{ ref('prep_session') }} s
    ON st.meeting_key = s.meeting_key
    AND st.session_key = s.session_key
    AND st.driver_number = s.driver_number

LEFT JOIN {{ ref('prep_weather_control') }} w
    ON st.session_key = w.session_key

LEFT JOIN {{ ref('prep_result') }} r 
    ON st.session_key = r.session_key 
    AND st.driver_number = r.driver_number

ORDER BY st.session_key, st.driver_number, st.stint_number