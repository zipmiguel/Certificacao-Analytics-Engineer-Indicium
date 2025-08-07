with 
    source as (
        select * 
        from {{ source('erp', 'sales_store') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as pk_loja
            , cast(salespersonid as int) as fk_vendedor
            , cast(name as varchar) as nome_loja            
            --demographics,
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed