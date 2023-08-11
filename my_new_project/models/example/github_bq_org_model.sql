
{{ config(
    materialized='view',
    schema='dbt_models'
) }}

select type as event_type, created_year, repository.url, repository.description, actor_attributes.type 
from {{ ref('github_nested') }}  where actor_attributes.type = 'Organization'