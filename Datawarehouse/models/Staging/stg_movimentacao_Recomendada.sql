with source as (
    date,
    symbol,
    action,
    quantity
from 
    {{source('dbsales_17k0', 'mov_commo')}}
),

renamed as (
    select
        cast(date as date) as data,
        symbol as simbolo,
        action as acao,
        quantity as qtde
from source

)

Select * from renamed