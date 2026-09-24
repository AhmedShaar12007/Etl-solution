{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
  )
}}

SELECT
    o.order_id,
    o.customer_id,
    o.status,
    o.amount,
    CASE WHEN o.status = 'PAID'
         THEN o.amount ELSE 0 END AS paid_amount,
    CASE WHEN o.amount > 500
         THEN TRUE ELSE FALSE END AS is_high_value,
    o.updated_at
FROM {{ ref('stg_orders') }} o

{% if is_incremental() %}
WHERE o.updated_at > (
    SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz)
    FROM {{ this }}
)
{% endif %}