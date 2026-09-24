{{ config(materialized='table') }}

-- This reads directly from the landing zone created by dlt
SELECT 
    id,
    UPPER(product_name) as product,
    amount * 1.15 as price_with_tax, -- simulated transformation
    sale_date::DATE as sale_day
FROM staging_data.sales_data
