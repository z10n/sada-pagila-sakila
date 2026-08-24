-- Customer dimension model enriched with location details
SELECT
    global_customer_id,
    customer_id,
    store_id,
    first_name,
    last_name,
    full_name,
    email,
    address,
    district,
    postal_code,
    phone,
    city,
    country_id,
    create_date,
    source_system
FROM {{ ref('int_customer_enriched') }}