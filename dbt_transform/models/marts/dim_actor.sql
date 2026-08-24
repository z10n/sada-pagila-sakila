-- Actor dimension model
SELECT
    global_actor_id,
    actor_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    source_system,
    last_update
FROM {{ ref('stg_actor') }}