-- Unified staging model for categories
WITH pagila AS (
    SELECT *, 'PAGILA' AS source_system FROM {{ source('pagila', 'category') }}
),
sakila AS (
    SELECT *, 'SAKILA' AS source_system FROM {{ source('sakila', 'category') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || category_id AS global_category_id,
    category_id,
    name AS category_name,
    last_update,
    source_system
FROM unioned