{% set all_departments = ["Office Supplies", "Furniture", "Technology"] %}

{{ 
    config(materialized='table', schema='dbt_models') 
}}


with superstore_cte as (
    select * from `local-talent-355615.dbt_staging_dataset.superstore_dbt_staging_table_parquet` 
)


select
    order_id,
    {% for department in all_departments %}
    sum(case when department = '{{department}}' then sales end) as {{ department | replace(' ', '_') | lower() }}_amount,
    {% endfor %}
    sum(sales) as total_sales_amount
from superstore_cte
group by order_id
