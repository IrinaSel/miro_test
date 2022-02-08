{{
    config(
        materialized = 'incremental',
        unique_key = 'user_id'
    )
}}
with sessions_all as (

    select * from {{ ref('stg_marketing__sessions')}} 
    {% if is_incremental() %}

    where time_started > (select date_add('hour', -12, max(registration_time)) from {{ this }}) 
    
    {% endif %}
),
conversions as (

    select * 
    from {{ ref('stg_marketing__conversions')}} 
    {% if is_incremental() %}

    where registration_time > (select max(registration_time) from {{ this }}) 
    
    {% endif %}
),
attribution_ordered as (
  select 
    conversions.user_id, 
    conversions.registration_time,
    sessions_all.medium,
    row_number() over(partition by conversions.user_id order by sessions_all.session_priority, sessions_all.time_started) as rn
  from conversions
    left join sessions_all on conversions.user_id = t.user_id and conversions.registration_time between sessions_all.time_started and sessions_all.time_ended
)
select 
    user_id, 
    registration_time,
    coalesce(medium, 'OTHER') as channel
from attribution_ordered
where rn=1





