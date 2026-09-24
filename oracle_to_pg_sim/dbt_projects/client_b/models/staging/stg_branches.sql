{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('staging', 'branches') }}
