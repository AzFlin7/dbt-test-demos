
{{ config(
    materialized='view',
    schema='dbt_models'
) }}


with github_timelines_cte as (
    
    select * from {{ ref('github_timelines') }}

)

select event_type, created_year, repository_url as url, repository_description as description, 
actor_attributes_type as type
from github_timelines_cte
where actor_attributes_type = 'Organization'