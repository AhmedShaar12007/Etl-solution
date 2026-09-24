
      
        
        
        delete from "warehouse"."marts_marts"."fct_orders" as DBT_INTERNAL_DEST
        where (order_id) in (
            select distinct order_id
            from "fct_orders__dbt_tmp084012511834" as DBT_INTERNAL_SOURCE
        );

    

    insert into "warehouse"."marts_marts"."fct_orders" ("order_id", "customer_id", "customer_name", "customer_country", "customer_tier", "status", "amount", "product_code", "paid_amount", "pending_amount", "is_high_value", "order_date", "order_week", "order_month", "created_at", "updated_at", "loaded_at")
    (
        select "order_id", "customer_id", "customer_name", "customer_country", "customer_tier", "status", "amount", "product_code", "paid_amount", "pending_amount", "is_high_value", "order_date", "order_week", "order_month", "created_at", "updated_at", "loaded_at"
        from "fct_orders__dbt_tmp084012511834"
    )
  