with 
    source as (
        select * 
        from {{ source('erp', 'person_address') }}
    )
    , renamed as (
        select
            cast(addressid as int) as pk_endereco
            , cast(stateprovinceid as varchar) fk_estado
            , cast(city as varchar) as nome_cidade
            , cast(addressline1 as varchar) as endereco          
            , cast(postalcode as varchar) as codigo_postal
            --addressline2,
            --spatiallocation,
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed