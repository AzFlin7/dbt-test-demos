{{ config(
    materialized='table',
    schema = 'dbt_models',
    partition_by={"field": "created_year", "data_type": "int64"
                  , "range": {"start": 2000, "end": 9999, "interval": 1}
                  }
)}}

select 
  distinct coalesce(type, 'Unknown') as event_type, 
  coalesce(cast(substr(repository_created_at, 1, 4) as int), 9999) as created_year, *
from {{ ref('github_timelines_stage') }} limit 10000