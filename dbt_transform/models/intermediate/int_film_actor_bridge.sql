-- Bridge model connecting films and actors across both systems
WITH pagila_film_actor AS (
    SELECT
        'PAGILA' AS source_system,
        actor_id,
        film_id,
        last_update
    FROM {{ source('pagila', 'film_actor') }}
),
sakila_film_actor AS (
    SELECT
        'SAKILA' AS source_system,
        actor_id,
        film_id,
        last_update
    FROM {{ source('sakila', 'film_actor') }}
),
unioned AS (
    SELECT * FROM pagila_film_actor
    UNION ALL
    SELECT * FROM sakila_film_actor
)
SELECT
    source_system || '_' || actor_id || '_' || film_id AS bridge_id,
    source_system || '_' || actor_id AS global_actor_id,
    source_system || '_' || film_id AS global_film_id,
    actor_id,
    film_id,
    source_system,
    last_update
FROM unioned