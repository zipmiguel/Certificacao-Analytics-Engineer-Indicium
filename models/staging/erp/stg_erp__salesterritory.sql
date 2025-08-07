with 
    source as (
        select * 
        from {{ source('erp', 'sales_salesterritory') }}
    )
    , renamed as (
        select
            cast(territoryid as int) as pk_territorio
            , cast(countryregioncode as varchar) as fk_pais
            , cast(name as varchar) as nome_regiao_pais            
            , cast("group" as varchar) as nome_regiao_continente
            , cast(salesytd as numeric(18,2)) as vendas_ano_atual_territorio -- Valor total em vendas acumuladas no ano atual por território
            , cast(saleslastyear as numeric(18,2)) as vendas_ano_anterior_territorio -- Valor total em vendas acumuladas no ano passado por território
            --costytd,
            --costlastyear,
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed