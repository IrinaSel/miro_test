with source as (

    select * from {{ source('marketing', 'sessions') }}

)

select 
    {{ dbt_utils.surrogate_key(['user_id', 'time_started']) }} as session_id,
    *,
    case 
        when is_paid = true and medium = 'DISPLAY' then dateadd(hour, 1, time_started) 
        when is_paid = true then dateadd(hour, 3, time_started)  
        else dateadd(hour, 12, time_started) 
    end as time_ended,
    case when is_paid = true then 1 
            when medium = 'DIRECT' then 3
            else 2 end as session_priority 
from source 