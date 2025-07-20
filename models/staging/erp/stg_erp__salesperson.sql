with 
    source as (
        select * 
        from {{ source('erp', 'salesperson') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as pk_vendedor
            , cast(territoryid as int) as fk_territorio
            , cast(salesquota as numeric(18,2)) as cota_vendas -- Valor de cota de vendas estipulada para o vendedor
            , cast(bonus as numeric(18,2)) as bonus -- Bonificação do vendedor com base nas vendas
            , cast(commissionpct as numeric(18,2)) as percentual_comissao -- Percentual de comissão sobre vendas realizadas
            , cast(salesytd as numeric(18,2)) as vendas_ano_atual_vendedor -- Valor total em vendas acumuladas no ano atual por vendedor
            , cast(saleslastyear as numeric(18,2)) as vendas_ano_anterior_vendedor -- Valor total em vendas acumuladas no ano passado por vendedor
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed