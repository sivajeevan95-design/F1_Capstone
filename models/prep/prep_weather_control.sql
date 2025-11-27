

with race_control as (
    select * from {{ ref('stg_race_control') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),


safety_car_counts as (
    select
        session_key,
        count(*) as safety_car
    from race_control
    
    where message = 'SAFETY CAR DEPLOYED'
    group by 1
),


weather_aggregates as (
    select
        session_key,
        avg(air_temperature) as avg_air_temp,
        avg(track_temperature) as avg_track_temp,
        avg(humidity) as avg_humidity,
        -- Correcting the typo from the image 'acg_pressure' to 'avg_pressure'
        avg(pressure) as avg_pressure,
        avg(wind_speed) as avg_wind_speed,
        
        max(rainfall) >= 1 as rainfall_bool
    from weather
    group by 1
)


select
    w.session_key,
    
    coalesce(s.safety_car, 0) as safety_car,
    w.avg_air_temp,
    w.avg_track_temp,
    w.rainfall_bool,
    w.avg_humidity,
    w.avg_pressure,
    w.avg_wind_speed
from weather_aggregates w
left join safety_car_counts s 
    on w.session_key = s.session_key