with 
    source as (
        select * 
        from {{ source('erp', 'salesreason') }}
    )
    , renamed as (
        select
            cast(salesreasonid as int) as pk_motivo_venda
            , cast(name as varchar) as motivo_venda
            , cast(reasontype as varchar) as tipo_motivo_venda
            --modifieddate
        from source
    )
select * 
from renamed