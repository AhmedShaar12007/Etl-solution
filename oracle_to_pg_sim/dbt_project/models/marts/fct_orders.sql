-- =============================================================================
-- models/marts/fct_orders.sql
-- =============================================================================
-- INCREMENTAL fact table — the core delta model.
--
-- How delta works here:
--   First run  → processes ALL rows from stg_orders (full load)
--   Every run after → the is_incremental() block injects a WHERE clause that
--                     only picks up rows whose _dlt_loaded_at is NEWER than
--                     the highest loaded_at already in this table.
--
-- This means DBT only transforms rows that DLT loaded THIS RUN.
-- The merge strategy then upserts them: new rows inserted, changed rows updated.
-- =============================================================================

{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
  )
}}
with source as (
    select
        o.order_id,
        o.customer_id,
        o.status,
        o.amount,
        o.product_code,
        o.created_at,
        o.updated_at,
        o._dlt_load_id,
        c.full_name     as customer_name,
        c.country       as customer_country,
        c.tier          as customer_tier
    from {{ ref('stg_orders') }} as o
    left join {{ ref('stg_customers') }} as c
        on o.customer_id = c.customer_id

  {% if is_incremental() %}
    where o.updated_at > (
        select coalesce(max(updated_at), '1970-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
)

select
    order_id,
    customer_id,
    customer_name,
    customer_country,
    customer_tier,
    status,
    amount,
    product_code,
    case when status = 'PAID'    then amount else 0 end  as paid_amount,
    case when status = 'PENDING' then amount else 0 end  as pending_amount,
    case when amount > 500       then true  else false end as is_high_value,
    date_trunc('day',  created_at)  as order_date,
    date_trunc('week', created_at)  as order_week,
    date_trunc('month',created_at)  as order_month,
    created_at,
    updated_at,
    updated_at as loaded_at
from source