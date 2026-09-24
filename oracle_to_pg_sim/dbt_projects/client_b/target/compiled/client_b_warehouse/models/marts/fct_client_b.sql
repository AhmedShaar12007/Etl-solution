

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
FROM "warehouse"."marts_staging"."stg_orders" o


WHERE o.updated_at > (
    SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz)
    FROM "warehouse"."marts_marts"."fct_client_b"
)
