
  create view "warehouse"."marts_staging"."stg_branches__dbt_tmp"
    
    
  as (
    

SELECT
    *
FROM "warehouse"."staging"."branches"
  );