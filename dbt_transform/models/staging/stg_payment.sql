-- Unified staging model for payments
WITH pagila AS (
    SELECT
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        amount,
        payment_date,
        'PAGILA' AS source_system
    FROM {{ source('pagila', 'payment') }}
),
sakila AS (
    SELECT
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        amount,
        payment_date,
        'SAKILA' AS source_system
    FROM {{ source('sakila', 'payment') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || payment_id AS global_payment_id,
    payment_id,
    source_system || '_' || customer_id AS global_customer_id,
    staff_id,
    source_system || '_' || rental_id AS global_rental_id,
    amount,
    payment_date,
    source_system
FROM unioned