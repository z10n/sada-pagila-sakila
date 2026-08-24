-- Unified staging model for cities
WITH pagila AS (
    SELECT *, 'PAGILA' AS source_system FROM {{ source('pagila', 'city') }}
),
sakila AS (
    SELECT *, 'SAKILA' AS source_system FROM {{ source('sakila', 'city') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || city_id AS global_city_id,
    city_id,
    city,
    country_id,
    last_update,
    source_system
FROM unioned