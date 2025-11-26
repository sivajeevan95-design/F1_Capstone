WITH stint_features AS (
    SELECT 
        meeting_key,
        session_key,
        driver_number,

        COUNT(*) AS stint_count,

        (COUNT(*) - 1) AS pit_count,

        STRING_AGG(
            CONCAT(stint_number, ':', compound),
            '>' ORDER BY stint_number
        ) AS compound_sequence,

        AVG(lap_end - lap_start + 1) AS avg_stint_length,
        SUM(lap_end - lap_start + 1) AS total_race_laps,
        MIN(lap_end - lap_start + 1) AS min_stint_length,
        MAX(lap_end - lap_start + 1) AS max_stint_length

    FROM {{ ref('prep_stints') }}
    GROUP BY meeting_key, session_key, driver_number
),

strategy_bins AS (
    SELECT 
        meeting_key,
        session_key,
        driver_number,

        stint_count,
        pit_count,
        compound_sequence,
        avg_stint_length,
        total_race_laps,
        min_stint_length,
        max_stint_length,

        CASE 
            WHEN avg_stint_length <= 12 THEN 'Short'
            WHEN avg_stint_length <= 25 THEN 'Medium'
            ELSE 'Long'
        END AS stint_length_bin,

        CASE 
            WHEN pit_count = 0 THEN 'No-stop'
            WHEN pit_count = 1 THEN '1-stop'
            WHEN pit_count = 2 THEN '2-stop'
            ELSE '3+-stop'
        END AS pit_bin

    FROM stint_features
)

SELECT 
    s.*,
    sb.stint_count,
    sb.pit_count,
    sb.compound_sequence,
    sb.avg_stint_length,
    sb.total_race_laps,
    sb.min_stint_length,
    sb.max_stint_length,
    sb.stint_length_bin,
    sb.pit_bin

FROM {{ref('prep_session')}} s
LEFT JOIN strategy_bins sb
USING (meeting_key, session_key, driver_number)
