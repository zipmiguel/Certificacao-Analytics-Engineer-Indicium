with 
    stg_erp__salesorderheadersalesreason as (
    select *
    from {{ref('stg_erp__salesorderheadersalesreason')}}
    )

    , stg_erp__salesreason as (
    select *
    from {{ref('stg_erp__salesreason')}}
    )

    , motivos_venda_por_pedido as (
    select 
        stg_erp__salesorderheadersalesreason.fk_pedido
        , stg_erp__salesreason.motivo_venda as motivo_venda
    from stg_erp__salesorderheadersalesreason
    left join stg_erp__salesreason on stg_erp__salesorderheadersalesreason.fk_motivo_venda = stg_erp__salesreason.pk_motivo_venda
    )

    , final as (
    select
        {{ dbt_utils.generate_surrogate_key(['fk_pedido'])}} as sk_pedido_venda
        , fk_pedido
        -- função abaixo usada para concatenar múltiplos motivos de venda do mesmo pedido em uma única string separada por vírgulas
        , listagg(motivo_venda, ', ') within group (order by motivo_venda asc) as motivos_venda_concatenados
    from motivos_venda_por_pedido
    group by fk_pedido
    )
select *
from final