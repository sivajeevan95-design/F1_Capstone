select *
FROM {{ source('f1_data', 'raw_weather') }}