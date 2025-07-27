with 
    stg_erp__salesorderheader as (
        select 
            distinct(fk_cartao_credito)
        from {{ref('stg_erp__salesorderheader')}}
        where fk_cartao_credito is not null
    )

    , stg_erp__creditcard as (
    select *
    from {{ref('stg_erp__creditcard')}}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['fk_cartao_credito']) }} as sk_cartao_credito
            , stg_erp__salesorderheader.fk_cartao_credito as id_cartao_credito
            , stg_erp__creditcard.tipo_cartao
        from stg_erp__salesorderheader
        left join stg_erp__creditcard 
            on stg_erp__salesorderheader.fk_cartao_credito = stg_erp__creditcard.pk_cartao_credito
    )
select *
from final