{% snapshot orders_snapshot %}


{{
    config(
      target_database='local-talent-355615',
      target_schema='snapshot_data',
      
      unique_key='primary_key', 

      strategy='check', 
      check_cols=['order_priority'] 
    )
}}

select *,
concat(customer_id, '-', order_id, '-', item_id) as primary_key
from 
{{ ref('superstore_model') }}

{% endsnapshot %}