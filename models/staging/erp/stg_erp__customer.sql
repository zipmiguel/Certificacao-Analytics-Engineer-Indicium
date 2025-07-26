with 
    source as (
        select * 
        from {{ source('erp', 'sales_customer') }}
    )
    , renamed as (
        select
            cast(customerid as int) as pk_cliente
            , cast(personid as int) as fk_pessoa
            , cast(storeid as int) as fk_loja
            , cast(territoryid as int) as fk_territorio
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed