-- Unified staging model for rentals
WITH pagila AS (
    SELECT
        rental_id,
        rental_date,
        inventory_id,
        customer_id,
        return_date,
        staff_id,
        last_update,
        'PAGILA' AS source_system
    FROM {{ source('pagila', 'rental') }}
),
sakila AS (
    SELECT
        rental_id,
        rental_date,
        inventory_id,
        customer_id,
        return_date,
        staff_id,
        last_update,
        'SAKILA' AS source_system
    FROM {{ source('sakila', 'rental') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || rental_id AS global_rental_id,
    rental_id,
    rental_date,
    source_system || '_' || inventory_id AS global_inventory_id,
    source_system || '_' || customer_id AS global_customer_id,
    return_date,
    staff_id,
    last_update,
    source_system
FROM unioned