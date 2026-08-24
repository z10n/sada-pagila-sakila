-- Enriched customer information with geographic details
WITH customers AS (
    SELECT * FROM {{ ref('stg_customer') }}
),
addresses AS (
    SELECT * FROM {{ ref('stg_address') }}
),
cities AS (
    SELECT * FROM {{ ref('stg_city') }}
)
SELECT
    c.global_customer_id,
    c.customer_id,
    c.store_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.global_address_id,
    a.address,
    a.district,
    a.postal_code,
    a.phone,
    a.global_city_id,
    ci.city,
    ci.country_id,
    c.create_date,
    c.source_system
FROM customers c
LEFT JOIN addresses a
    ON c.global_address_id = a.global_address_id
LEFT JOIN cities ci
    ON a.global_city_id = ci.global_city_id