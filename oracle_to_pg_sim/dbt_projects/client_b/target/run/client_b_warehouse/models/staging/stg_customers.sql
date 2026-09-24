
  create view "warehouse"."marts_staging"."stg_customers__dbt_tmp"
    
    
  as (
    

SELECT
    *
FROM "warehouse"."staging"."customers"
  );