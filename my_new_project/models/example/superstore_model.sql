{{ 
    config(materialized='table', schema='dbt_models') 
}}

with superstore_stage as (
    select *
    from `local-talent-355615.dbt_staging_dataset.superstore_dbt_staging_table_parquet` 
)

select
    *
from superstore_stage
