with 
    source as (
        select * 
        from {{ source('erp', 'salesorderheadersalesreason') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as fk_pedido
            , cast(salesreasonid as int) as fk_motivo_venda
            --modifieddate
        from source
    )
select * 
from renamed