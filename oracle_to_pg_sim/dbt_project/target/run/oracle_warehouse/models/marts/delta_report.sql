
      insert into "warehouse"."marts_marts"."delta_report" ("order_id", "customer_id", "status", "amount", "product_code", "changed_at_source", "detected_at", "load_date", "change_type")
    (
        select "order_id", "customer_id", "status", "amount", "product_code", "changed_at_source", "detected_at", "load_date", "change_type"
        from "delta_report__dbt_tmp084012515342"
    )


  