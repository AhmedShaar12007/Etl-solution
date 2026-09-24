-- =============================================================================
-- models/marts/delta_report.sql
-- =============================================================================
-- Append-only log of every row that was new or changed in each DLT run.
-- Your team queries this to monitor what moved through the pipeline.
--
-- Query to check today's delta:
--   SELECT * FROM marts.delta_report WHERE load_date = CURRENT_DATE ORDER BY detected_at DESC;
-- =============================================================================
{{
  config(
    materialized='incremental',
    incremental_strategy='append'
  )
}}

select
    o.order_id,
    o.customer_id,
    o.status,
    o.amount,
    o.product_code,
    o.updated_at                        as changed_at_source,
    o.updated_at                        as detected_at,
    cast(o.updated_at as date)          as load_date,
    case
        when o.updated_at = o.created_at then 'new_row'
        else 'updated_row'
    end                                 as change_type

from {{ ref('stg_orders') }} as o

{% if is_incremental() %}
where o.updated_at > (
    select coalesce(max(detected_at), '1970-01-01'::timestamptz)
    from {{ this }}
)
{% endif %}