-- Distribution of films by category
SELECT
    COALESCE(category_name, 'Unknown') AS category_name,
    COUNT(global_film_id) AS total_films
FROM {{ ref('dim_film') }}
GROUP BY 1
ORDER BY total_films DESC