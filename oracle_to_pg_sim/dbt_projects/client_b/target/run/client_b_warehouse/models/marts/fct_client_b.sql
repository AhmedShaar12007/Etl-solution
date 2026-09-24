
      
        
        
        delete from "warehouse"."marts_marts"."fct_client_b" as DBT_INTERNAL_DEST
        where (order_id) in (
            select distinct order_id
            from "fct_client_b__dbt_tmp195444234305" as DBT_INTERNAL_SOURCE
        );

    

    insert into "warehouse"."marts_marts"."fct_client_b" ("order_id", "customer_id", "status", "amount", "paid_amount", "is_high_value", "updated_at")
    (
        select "order_id", "customer_id", "status", "amount", "paid_amount", "is_high_value", "updated_at"
        from "fct_client_b__dbt_tmp195444234305"
    )
  