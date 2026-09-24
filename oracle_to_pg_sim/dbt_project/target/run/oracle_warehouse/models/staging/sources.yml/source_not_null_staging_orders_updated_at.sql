
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select updated_at
from "warehouse"."staging"."orders"
where updated_at is null



  
  
      
    ) dbt_internal_test