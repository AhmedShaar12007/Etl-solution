{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('staging', 'products') }}
