-- Enriched rental events with inventory, film, and payment metrics
WITH rentals AS (
    SELECT * FROM {{ ref('stg_rental') }}
),
inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),
payments AS (
    SELECT
        global_rental_id,
        SUM(amount) AS total_paid_amount
    FROM {{ ref('stg_payment') }}
    GROUP BY 1
)
SELECT
    r.global_rental_id,
    r.rental_id,
    r.rental_date,
    r.return_date,
    r.global_customer_id,
    r.global_inventory_id,
    i.global_film_id,
    r.staff_id,
    r.source_system,
    -- Calculate rental duration in hours
    DATEDIFF('hour', r.rental_date, r.return_date) AS rental_duration_hours,
    COALESCE(p.total_paid_amount, 0) AS rental_amount
FROM rentals r
LEFT JOIN inventory i
    ON r.global_inventory_id = i.global_inventory_id
LEFT JOIN payments p
    ON r.global_rental_id = p.global_rental_id