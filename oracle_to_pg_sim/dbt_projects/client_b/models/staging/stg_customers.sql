{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('staging', 'customers') }}
