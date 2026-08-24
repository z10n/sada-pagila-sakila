-- Unified staging model for addresses
WITH pagila AS (
    SELECT *, 'PAGILA' AS source_system FROM {{ source('pagila', 'address') }}
),
sakila AS (
    SELECT *, 'SAKILA' AS source_system FROM {{ source('sakila', 'address') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || address_id AS global_address_id,
    address_id,
    address,
    address2,
    district,
    source_system || '_' || city_id AS global_city_id,
    postal_code,
    phone,
    last_update,
    source_system
FROM unioned