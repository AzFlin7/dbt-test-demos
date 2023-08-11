{{ 
    config(materialized='table', schema='dbt_models') 
}}

{%- set transform_col_list = ["repository_url", "repository_created_at", "url", "type"] -%}

{%- set max_over_window_col_list = ["repository_forks", "repository_size", "repository_open_issues", "repository_watchers"] -%}

{%- set relation = api.Relation.create(database='bigquery-public-data', schema='samples', identifier='github_timeline', type= 'table') -%}

{%- set column_list = adapter.get_columns_in_relation(relation) -%}


-- Imports using CTE's
with github_timeline as (
    select * from `bigquery-public-data.samples.github_timeline` where type = '{{ env_var("EVENT_TYPE", "None") }}'
),

view_domain_name as (
    select * from {{ ref('xwalk_domain_names') }} do left join {{ ref('ref_domain_names') }} rf
    on do.ref_domain_key = rf.domain_key
),

github_timeline_simple as ( 
    select distinct
            CONCAT(repository_url , '_' , repository_created_at , '_' , repository_pushed_at , '_' , actor_attributes_gravatar_id , '_' , created_at  ) as primary_key,
            TO_HEX(MD5(CONCAT(repository_url , '_' , repository_created_at , '_' , repository_pushed_at , '_' , actor_attributes_gravatar_id , '_' , created_at  ))) as hash_primary_key,
            {{ dbt_common_utils.trim_whitespaces('repository_url') }} as repository_url,
            {{ dbt_common_utils.trim_whitespaces('repository_created_at') }} as repository_created_at,
            {{ dbt_common_utils.trim_whitespaces('url') }} as url,
            {{ dbt_common_utils.trim_whitespaces('type') }} as type,
            ARRAY_REVERSE(SPLIT(actor_attributes_email, '.'))[SAFE_OFFSET(0)] as email_domain,
            
            -- Window functions for max calculations
            {% for column_name in max_over_window_col_list %}
            max({{column_name}}) over (partition by repository_url) as max_{{column_name}},
            {% endfor %}
            
            -- Logic to list all the remaining cols that are not transformed
            {% for column_name in column_list %}
            {% if column_name.name not in transform_col_list %}
                {{ column_name.name }}  as {{ column_name.name }},
            {% endif %}
            {% endfor %}
    from github_timeline
)


select 
    struct(gt.repository_url as base_url, gt.max_repository_forks as forks, gt.max_repository_size as size, 
    gt.repository_open_issues as open_issues, gt.repository_watchers as watchers) as repo_stats,
    gt.*,
    case when gt.email_domain is not null then coalesce(cast(vd.domain_key as string), 'Error') end as domain_key,
    case when gt.email_domain is not null then coalesce(vd.domain_name, 'Error') else vd.domain_name end as domain_name 
from github_timeline_simple gt
left join view_domain_name vd on gt.email_domain = vd.source_domain_code
