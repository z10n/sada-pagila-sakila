-- Films that are missing from inventory
WITH films AS (
    SELECT global_film_id, title, source_system
    FROM {{ ref('dim_film') }}
),
inventory AS (
    SELECT DISTINCT global_film_id
    FROM {{ ref('stg_inventory') }}
)
SELECT
    f.global_film_id,
    f.title,
    f.source_system
FROM films f
LEFT JOIN inventory i ON f.global_film_id = i.global_film_id
WHERE i.global_film_id IS NULL