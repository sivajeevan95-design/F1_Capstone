WITH DriverStats AS (
    SELECT
        session_key,
        driver_number,
        FIRST_VALUE(position) OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY date ASC
        ) as start_pos,
        FIRST_VALUE(position) OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY date DESC
        ) as end_pos
    FROM {{source('f1_data', 'raw_position')}}
),
aggregated_positions AS (
    SELECT DISTINCT
        session_key,
        driver_number,
        start_pos,
        end_pos,
        (start_pos - end_pos) as pos_diff
    FROM DriverStats
)
SELECT
    s.*,                
    ap.driver_number,   
    ap.start_pos,
    ap.end_pos,
    ap.pos_diff      
FROM {{source('f1_data', 'raw_sessions')}} s
JOIN aggregated_positions ap 
    ON s.session_key = ap.session_key