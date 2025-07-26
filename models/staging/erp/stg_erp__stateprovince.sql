with 
    source as (
        select * 
        from {{ source('erp', 'person_stateprovince') }}
    )
    , renamed as (
        select
            cast(stateprovinceid as int) as pk_estado            
            , cast(countryregioncode as varchar) as fk_pais
            , cast(territoryid as int) as fk_territorio
            , cast(name as varchar) as nome_estado
            , cast(stateprovincecode as varchar) as codigo_estado
            --isonlystateprovinceflag,          
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed