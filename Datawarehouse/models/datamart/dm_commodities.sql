-- models/datamart/dm_commodities.sql

with commodities as (
    select
        data,
        simbolo,
        valor_fechamento
    from 
        {{ ref ('stg_commodities') }}
),

movimentacao as (
    select
        data,
        simbolo,
        acao,
        qtde
    from 
        {{ ref ('stg_movimentacao_Recomendada') }}
),

joined as (
    select
        c.data,
        c.simbolo,
        c.valor_fechamento,
        m.acao,
        m.qtde,
        (m.qtde * c.valor_fechamento) as valor,
        case
            when m.acao = 'sell' then (m.qtde * c.valor_fechamento)
            else -(m.qtde * c.valor_fechamento)
        end as ganho
    from
        commodities c
    inner join
        movimentacao m
    on
        c.data = m.data
        and c.simbolo = m.simbolo
),

last_day as (
    select
        max(data) as max_date
    from
        joined
),

filtered as (
    select
        *
    from
        joined
    where
        data = (select max_date from last_day)
)

select
    data,
    simbolo,
    valor_fechamento,
    acao,
    qtde as quantidade,
    valor,
    ganho
from
    filtered