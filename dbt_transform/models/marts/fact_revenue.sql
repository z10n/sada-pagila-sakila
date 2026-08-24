-- Revenue transactions fact table
SELECT
    global_payment_id,
    payment_id,
    global_customer_id,
    staff_id,
    global_rental_id,
    amount,
    payment_date,
    CAST(payment_date AS DATE) AS payment_date_id,
    source_system
FROM {{ ref('stg_payment') }}