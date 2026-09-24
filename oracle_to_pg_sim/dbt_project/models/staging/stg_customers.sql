-- =============================================================================
-- models/staging/stg_customers.sql
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    customer_id::INTEGER                AS customer_id,
    INITCAP(TRIM(full_name))            AS full_name,
    LOWER(TRIM(email))                  AS email,
    UPPER(TRIM(country))                AS country,
    UPPER(TRIM(tier))                   AS tier,   -- GOLD / SILVER / STANDARD

    created_at::TIMESTAMPTZ             AS created_at,
    updated_at::TIMESTAMPTZ             AS updated_at,

    _dlt_id                             AS _dlt_id,
    _dlt_load_id                      AS _dlt_load_id

FROM {{ source('staging', 'customers') }}
WHERE COALESCE(is_deleted, 0) = 0
