-- Top actors appearing in Children category films
WITH children_films AS (
    SELECT global_film_id
    FROM {{ ref('dim_film') }}
    WHERE LOWER(category_name) = 'children'
),
bridge AS (
    SELECT global_actor_id, global_film_id
    FROM {{ ref('int_film_actor_bridge') }}
),
actors AS (
    SELECT global_actor_id, full_name
    FROM {{ ref('dim_actor') }}
)
SELECT
    a.global_actor_id,
    a.full_name AS actor_name,
    COUNT(cf.global_film_id) AS children_film_count
FROM actors a
JOIN bridge b ON a.global_actor_id = b.global_actor_id
JOIN children_films cf ON b.global_film_id = cf.global_film_id
GROUP BY 1, 2
ORDER BY children_film_count DESC