-- =============================================================================
-- models/staging/stg_orders.sql
-- =============================================================================
-- Lightweight VIEW over the raw DLT-loaded orders table.
-- Purpose: cast types, rename columns, filter soft-deletes.
-- Materialized as a VIEW so it always reflects the latest DLT load.
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    order_id::INTEGER                   AS order_id,
    customer_id::INTEGER                AS customer_id,

    UPPER(TRIM(status))                 AS status,
    amount::NUMERIC(10,2)               AS amount,
    UPPER(TRIM(product_code))           AS product_code,

    created_at::TIMESTAMPTZ             AS created_at,
    updated_at::TIMESTAMPTZ             AS updated_at,

    -- DLT metadata columns — kept for delta filtering in marts
    _dlt_id                             AS _dlt_id,
    _dlt_load_id                      AS _dlt_load_id

FROM {{ source('staging', 'orders') }}

-- Filter soft-deleted rows (is_deleted=1 set in Oracle before export)
WHERE COALESCE(is_deleted, 0) = 0
