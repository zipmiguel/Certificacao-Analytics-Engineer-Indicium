with
    dim_cliente as (
        select
            *
        from {{ref('dim_cliente')}}
    )
    
    , dim_cartao_credito as (
        select 
            *
        from {{ ref("dim_cartao_credito") }}
    )   

    , dim_localizacao as (
        select
            *
        from {{ref('dim_localizacao')}}
    )

    , dim_motivo_venda as (
        select 
            *
        from {{ ref("dim_motivo_venda") }}
    )

    , dim_produto as (
        select
            *
        from {{ref('dim_produto')}}
    )

    , stg_erp__salesorderdetail as (
        select 
            *
        from {{ref('stg_erp__salesorderdetail')}}
    )

    , stg_erp__salesorderheader as (
        select 
            *
        from {{ref('stg_erp__salesorderheader')}}
    )

    , int_detalhes_vendas as (
        select 
            * 
        from {{ ref("int_detalhes_vendas") }}
    )
    
    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_detalhes_pedido'])}} as sk_fato_vendas
            , int_detalhes_vendas.sk_detalhes_vendas as fk_detalhes_vendas
            , int_detalhes_vendas.pk_detalhes_pedido as fk_detalhes_pedido
            , int_detalhes_vendas.fk_pedido
            , int_detalhes_vendas.fk_produto
            , int_detalhes_vendas.fk_cliente            
            , int_detalhes_vendas.fk_vendedor
            , int_detalhes_vendas.fk_territorio
            , int_detalhes_vendas.fk_endereco_entrega
            , int_detalhes_vendas.fk_cartao_credito
            , dim_motivo_venda.motivos_venda_concatenados
            , int_detalhes_vendas.data_pedido
            , int_detalhes_vendas.data_envio
            , int_detalhes_vendas.nome_status_pedido
            , int_detalhes_vendas.codigo_pedido
            , int_detalhes_vendas.sinalizador_pedido_online
            , int_detalhes_vendas.quantidade_produto
            , int_detalhes_vendas.preco_unitario_produto
            , int_detalhes_vendas.desconto_preco_unitario
            , int_detalhes_vendas.imposto_por_produto
            , int_detalhes_vendas.frete_por_produto 
            , int_detalhes_vendas.valor_total_bruto
            , int_detalhes_vendas.valor_total_liquido
            , dim_cliente.nome_completo
            , dim_cliente.nome_loja
            , dim_cliente.tipo_pessoa
        from int_detalhes_vendas
        left join dim_cliente on int_detalhes_vendas.fk_cliente = dim_cliente.pk_cliente
        left join dim_motivo_venda on int_detalhes_vendas.fk_pedido = dim_motivo_venda.fk_pedido
    )
select *
from final