
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _dlt_loaded_at
from "warehouse"."staging"."customers"
where _dlt_loaded_at is null



  
  
      
    ) dbt_internal_test