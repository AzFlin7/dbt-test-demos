 {{ config(
    materialized='table',
    schema = 'dbt_staging_dataset',
    partition_by = {
      "field": "created_date",
      "data_type": "DATE",
      "granularity": "year"
    }
 )
}}


select
  cast(substr(repository_created_at, 1, 10) as DATE FORMAT 'YYYY-MM-DD') as created_date, *
from `publicdata.samples.github_timeline` limit 1000

