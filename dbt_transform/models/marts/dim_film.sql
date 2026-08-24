-- Film dimension with category details aggregated to preserve 1 row per film
WITH films AS (
    SELECT * FROM {{ ref('stg_film') }}
),
categories AS (
    SELECT * FROM {{ ref('stg_category') }}
),
pagila_fc AS (
    SELECT 'PAGILA' AS source_system, film_id, category_id FROM {{ source('pagila', 'film_category') }}
),
sakila_fc AS (
    SELECT 'SAKILA' AS source_system, film_id, category_id FROM {{ source('sakila', 'film_category') }}
),
film_categories AS (
    SELECT * FROM pagila_fc UNION ALL SELECT * FROM sakila_fc
),
film_category_joined AS (
    SELECT
        fc.source_system,
        fc.film_id,
        LISTAGG(c.category_name, ', ') WITHIN GROUP (ORDER BY c.category_name) AS category_name
    FROM film_categories fc
    LEFT JOIN categories c
        ON fc.source_system || '_' || fc.category_id = c.global_category_id
    GROUP BY fc.source_system, fc.film_id
)
SELECT
    f.global_film_id,
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    f.language_id,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    COALESCE(fc.category_name, 'Unknown') AS category_name,
    f.source_system,
    f.last_update
FROM films f
LEFT JOIN film_category_joined fc
    ON f.film_id = fc.film_id AND f.source_system = fc.source_system