-- Distribution of customers by city and district
SELECT
    COALESCE(city, 'Unknown') AS city,
    COALESCE(district, 'Unknown') AS district,
    COUNT(global_customer_id) AS total_customers
FROM {{ ref('dim_customer') }}
GROUP BY 1, 2
ORDER BY total_customers DESC