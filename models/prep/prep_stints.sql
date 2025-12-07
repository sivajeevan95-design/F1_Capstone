SELECT
    s.*,
    p_start.position AS start_pos,
    p_end.position AS end_pos,
    (p_start.position - p_end.position) AS stint_pos_dif
FROM {{ ref('stg_stints') }} s
LEFT JOIN {{ ref('stg_pos_laps') }} p_start
    ON s.meeting_key = p_start.meeting_key
    AND s.session_key = p_start.session_key
    AND s.driver_number = p_start.driver_number
    AND p_start.lap_number = (s.lap_start - 1)
LEFT JOIN {{ ref('stg_pos_laps') }} p_end
    ON s.meeting_key = p_end.meeting_key
    AND s.session_key = p_end.session_key
    AND s.driver_number = p_end.driver_number
    AND p_end.lap_number = s.lap_end