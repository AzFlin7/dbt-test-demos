{% set col_cast_to_string = ["repository_url", "repository_description", "repository_homepage", "repository_name"] %}

{% set col_cast_to_bool = ["repository_has_downloads", "repository_fork", "repository_has_wiki", "repository_private"] %}

{% set col_cast_to_int = ["repository_size", "repository_forks"] %}

{% set col_cast_to_datetime = ["repository_created_at"] %}

{{ 
    config(materialized='table', schema='dbt_models') 
}}


with github_timeline as (
    select * from `bigquery-public-data.samples.github_timeline` 
)


select
    {% for col in col_cast_to_string %}
    cast( {{ col }} as string) as {{ col }},
    {% endfor %}

    {% for col in col_cast_to_bool %}
    cast( {{ col }} as boolean) as {{ col }},
    {% endfor %}
    
    {% for col in col_cast_to_int %}
    cast( {{ col }} as int) as {{ col }},
    {% endfor %}

    
from github_timeline
