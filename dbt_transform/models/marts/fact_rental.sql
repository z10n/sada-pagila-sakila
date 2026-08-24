-- Rental transactions fact table
SELECT
    global_rental_id,
    rental_id,
    rental_date,
    CAST(rental_date AS DATE) AS rental_date_id,
    return_date,
    CAST(return_date AS DATE) AS return_date_id,
    global_customer_id,
    global_inventory_id,
    global_film_id,
    staff_id,
    rental_duration_hours,
    rental_amount,
    source_system
FROM {{ ref('int_rental_facts') }}