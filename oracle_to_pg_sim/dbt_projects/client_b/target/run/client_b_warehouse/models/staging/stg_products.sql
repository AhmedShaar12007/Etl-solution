
  create view "warehouse"."marts_staging"."stg_products__dbt_tmp"
    
    
  as (
    

SELECT
    *
FROM "warehouse"."staging"."products"
  );