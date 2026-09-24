
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select loaded_at
from "warehouse"."marts_marts"."fct_orders"
where loaded_at is null



  
  
      
    ) dbt_internal_test