select
    s.*,
    d.full_name,
    d.driver_number,
    d.headshot_url,
    p.start_pos,
    p.end_pos,
    p.pos_diff
from {{ ref("stg_session") }} s
join {{ ref("stg_position") }} p
    using (session_key)
join {{ ref("stg_drivers") }} d
    using (session_key, driver_number)
order by s.session_key, d.driver_number
