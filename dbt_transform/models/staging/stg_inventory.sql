-- Unified staging model for inventory
WITH pagila AS (
    SELECT *, 'PAGILA' AS source_system FROM {{ source('pagila', 'inventory') }}
),
sakila AS (
    SELECT *, 'SAKILA' AS source_system FROM {{ source('sakila', 'inventory') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || inventory_id AS global_inventory_id,
    inventory_id,
    source_system || '_' || film_id AS global_film_id,
    store_id,
    last_update,
    source_system
FROM unioned