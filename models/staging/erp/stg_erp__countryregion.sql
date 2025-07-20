with 
    source as (
        select * 
        from {{ source('erp', 'countryregion') }}
    )
    , renamed as (
        select
            countryregioncode as pk_pais
            , cast(name as varchar) as nome_pais
            --modifieddate
        from source
    )
select * 
from renamed