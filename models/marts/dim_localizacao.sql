with 
    stg_erp__salesorderheader as (
        select 
            distinct(fk_endereco_entrega)
        from {{ref('stg_erp__salesorderheader')}}
    )

    , stg_erp__address as (
    select *
    from {{ref('stg_erp__address')}}
    )

    , stg_erp__stateprovince as (
    select *
    from {{ref('stg_erp__stateprovince')}}
    )

    , stg_erp__countryregion as (
    select *
    from {{ref('stg_erp__countryregion')}}
    )

    , final as (
    select        
        {{ dbt_utils.generate_surrogate_key(['fk_endereco_entrega'])}} as sk_endereco_entrega
        , stg_erp__salesorderheader.fk_endereco_entrega 
        , stg_erp__address.nome_cidade as cidade
        , stg_erp__stateprovince.nome_estado as estado
        , stg_erp__countryregion.nome_pais as pais
    from stg_erp__salesorderheader
    left join stg_erp__address on stg_erp__salesorderheader.fk_endereco_entrega = stg_erp__address.pk_endereco
    left join stg_erp__stateprovince on stg_erp__address.fk_estado = stg_erp__stateprovince.pk_estado
    left join stg_erp__countryregion on stg_erp__stateprovince.fk_pais = stg_erp__countryregion.pk_pais 
    )
select *
from final