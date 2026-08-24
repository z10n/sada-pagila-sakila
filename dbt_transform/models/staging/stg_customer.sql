-- Unified staging model for customers
WITH pagila AS (
    SELECT
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        create_date,
        last_update,
        'PAGILA' AS source_system
    FROM {{ source('pagila', 'customer') }}
),
sakila AS (
    SELECT
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        create_date,
        last_update,
        'SAKILA' AS source_system
    FROM {{ source('sakila', 'customer') }}
),
unioned AS (
    SELECT * FROM pagila UNION ALL SELECT * FROM sakila
)
SELECT
    source_system || '_' || customer_id AS global_customer_id,
    customer_id,
    store_id,
    first_name,
    last_name,
    email,
    source_system || '_' || address_id AS global_address_id,
    create_date,
    last_update,
    source_system
FROM unioned