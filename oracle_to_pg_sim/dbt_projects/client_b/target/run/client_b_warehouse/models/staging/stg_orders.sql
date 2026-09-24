
  create view "warehouse"."marts_staging"."stg_orders__dbt_tmp"
    
    
  as (
    

SELECT
    *
FROM "warehouse"."staging"."orders"
  );