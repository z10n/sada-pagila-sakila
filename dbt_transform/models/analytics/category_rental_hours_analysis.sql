-- Analysis of rental duration hours by category
WITH rentals AS (
    SELECT global_film_id, rental_duration_hours
    FROM {{ ref('fact_rental') }}
    WHERE rental_duration_hours IS NOT NULL
),
films AS (
    SELECT global_film_id, COALESCE(category_name, 'Unknown') AS category_name
    FROM {{ ref('dim_film') }}
)
SELECT
    f.category_name,
    ROUND(AVG(r.rental_duration_hours), 2) AS avg_rental_hours,
    SUM(r.rental_duration_hours) AS total_rental_hours
FROM rentals r
JOIN films f ON r.global_film_id = f.global_film_id
GROUP BY 1
ORDER BY total_rental_hours DESC