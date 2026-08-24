-- Unified staging model for films
WITH pagila AS (
    SELECT
        film_id,
        title,
        description,
        release_year,
        language_id,
        rental_duration,
        rental_rate,
        length,
        replacement_cost,
        rating,
        last_update,
        'PAGILA' AS source_system
    FROM {{ source('pagila', 'film') }}
),
sakila AS (
    SELECT
        film_id,
        title,
        description,
        release_year,
        language_id,
        rental_duration,
        rental_rate,
        length,
        replacement_cost,
        rating,
        last_update,
        'SAKILA' AS source_system
    FROM {{ source('sakila', 'film') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || film_id AS global_film_id,
    film_id,
    title,
    description,
    release_year,
    language_id,
    rental_duration,
    rental_rate,
    length,
    replacement_cost,
    rating,
    last_update,
    source_system
FROM unioned