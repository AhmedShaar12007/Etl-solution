{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    o.order_id,
    o.status,
    o.amount,
    o.updated_at                        AS changed_at_source,
    CAST(o.updated_at AS DATE)          AS load_date,
    CASE
        WHEN o.updated_at = o.created_at THEN 'new_row'
        ELSE 'updated_row'
    END                                 AS change_type

FROM {{ ref('stg_orders') }} o

{% if is_incremental() %}
WHERE o.updated_at > (
    SELECT COALESCE(MAX(changed_at_source), '1970-01-01'::timestamptz)
    FROM {{ this }}
)
{% endif %}