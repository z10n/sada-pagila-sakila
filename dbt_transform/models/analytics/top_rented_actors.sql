-- Top rented actors based on total rental counts
WITH rentals AS (
    SELECT global_film_id, COUNT(*) AS rental_count
    FROM {{ ref('fact_rental') }}
    GROUP BY 1
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
    SUM(r.rental_count) AS total_rentals
FROM actors a
JOIN bridge b ON a.global_actor_id = b.global_actor_id
JOIN rentals r ON b.global_film_id = r.global_film_id
GROUP BY 1, 2
ORDER BY total_rentals DESC