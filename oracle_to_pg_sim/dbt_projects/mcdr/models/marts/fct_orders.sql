{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}

SELECT
    *
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at),'1970-01-01'::timestamptz) FROM {{ this }})
{% endif %}