{{ 
  config(
    materialized='table',
    schema = 'dbt_models',
    partition_by={"field": "created_year", "data_type": "int64"
                  , "range": {"start": 2000, "end": 9999, "interval": 1}
                  }
  )
}}

select  
  coalesce(cast(substr(repository.created_at, 1, 4) as int), 9999) as created_year, *
from `local-talent-355615.dbt_staging_dataset.github_nested_dbt_stage` limit 10000