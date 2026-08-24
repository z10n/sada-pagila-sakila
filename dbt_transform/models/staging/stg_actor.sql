-- Unified staging model for actors
WITH pagila AS (
    SELECT *, 'PAGILA' AS source_system FROM {{ source('pagila', 'actor') }}
),
sakila AS (
    SELECT *, 'SAKILA' AS source_system FROM {{ source('sakila', 'actor') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || actor_id AS global_actor_id,
    actor_id,
    first_name,
    last_name,
    last_update,
    source_system
FROM unioned