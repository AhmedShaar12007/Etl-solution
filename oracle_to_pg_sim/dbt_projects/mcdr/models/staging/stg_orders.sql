{{ config(materialized='view') }}

SELECT
    customer_id::INTEGER,
    INITCAP(full_name)     AS full_name,
    LOWER(email)           AS email,
    tier,
    country,
    created_at::TIMESTAMPTZ
FROM {{ source('staging', 'customers') }}