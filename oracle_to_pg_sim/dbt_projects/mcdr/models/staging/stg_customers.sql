{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('staging', 'stg_customers') }}