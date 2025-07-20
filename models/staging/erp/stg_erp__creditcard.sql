with 
    source as (
        select * 
        from {{ source('erp', 'creditcard') }}
    )
    , renamed as (
        select
            cast(creditcardid as int) as pk_cartao_credito
            , cast(cardtype as varchar) as tipo_cartao
            , cast(cardnumber as varchar) as numero_cartao
            --expmonth,
            --expyear,
            --modifieddate
        from source
    )
select * 
from renamed