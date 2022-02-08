with source as (

    select * from {{ source('marketing', 'conversions') }}

)

select * 
from source 