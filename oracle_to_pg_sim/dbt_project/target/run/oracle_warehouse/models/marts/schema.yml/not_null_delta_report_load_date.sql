
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_date
from "warehouse"."marts_marts"."delta_report"
where load_date is null



  
  
      
    ) dbt_internal_test