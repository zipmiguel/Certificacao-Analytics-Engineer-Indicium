with 
    stg_erp__person as (
        select
            pk_pessoa
            , tipo_de_pessoa
            , nome_completo
        from {{ref('stg_erp__person')}}
    )

    , stg_erp__store as (
        select
            pk_loja
            , nome_loja
        from {{ref('stg_erp__store')}}
    )

    , stg_erp__customer as (
        select 
            pk_cliente
            , fk_pessoa
            , fk_loja
        from {{ref('stg_erp__customer')}}
    )

    -- Calcula a data do primeiro pedido de cada cliente, útil para análises de ciclo de vida do cliente, segmentação e métricas de retenção
    , primeiro_pedido as (
        select
            soh.fk_cliente,
            min(soh.data_pedido) as data_primeiro_pedido
        from {{ ref('stg_erp__salesorderheader') }} as soh
        group by soh.fk_cliente
    )

    -- Agrega dados do cliente com informações da pessoa e loja, incluindo a data do primeiro pedido; 
    -- classifica o tipo de pessoa usando a coluna já tratada em stg_erp__person.sql para simplificar a lógica final.
    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_cliente']) }} as sk_cliente
            , customer.pk_cliente
            , person.nome_completo
            , store.nome_loja
            , primeiro_pedido.data_primeiro_pedido
            , case 
                when customer.fk_pessoa is null and customer.fk_loja is not null then 'Loja'
                when customer.fk_pessoa is not null then person.tipo_de_pessoa
                else 'Desconhecido'
              end as tipo_pessoa
        from stg_erp__customer as customer
        left join stg_erp__person as person on person.pk_pessoa = customer.fk_pessoa
        left join stg_erp__store as store on store.pk_loja = customer.fk_loja
        left join primeiro_pedido on customer.pk_cliente = primeiro_pedido.fk_cliente
    )
select *
from final