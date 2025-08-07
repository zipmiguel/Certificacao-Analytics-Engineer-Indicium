with 
    source as (
        select * 
        from {{ source('erp', 'sales_salesorderheader') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as pk_pedido
            , cast(customerid as int) as fk_cliente
            , cast(salespersonid as int) as fk_vendedor
            , cast(territoryid as int) as fk_territorio
            , cast(shiptoaddressid as int) as fk_endereco_entrega
            , cast(shipmethodid as int) as fk_metodo_envio
            , cast(creditcardid as int) as fk_cartao_credito
            , orderdate::date as data_pedido --Converte orderdate de timestamp (data e hora) para date (apenas data)
            , shipdate::date as data_envio --Converte sellstartdate de timestamp (data e hora) para date (apenas data)
            , cast(status as int) as status_pedido
            , case 
                 when cast(status as int) = 1 then 'Em processamento'
                 when cast(status as int) = 2 then 'Aprovado'
                 when cast(status as int) = 3 then 'Em espera'
                 when cast(status as int) = 4 then 'Rejeitado'
                 when cast(status as int) = 5 then 'Enviado'
                 when cast(status as int) = 6 then 'Cancelado'
                 else 'sem_status'
              end as nome_status_pedido --mostra o que cada número do status_pedido significa
            , cast(onlineorderflag as boolean) as sinalizador_pedido_online --Se for TRUE o pedido foi no site, se for FALSE foi em uma loja (store)
            , cast(purchaseordernumber as varchar) as codigo_pedido
            , cast(subtotal as numeric(18,2)) as subtotal_devido_por_pedido --subtotal_devido_por_pedido = quantidade_produto * preco_unitario_produto [TOTAL DO PEDIDO] 
            , cast(taxamt as numeric(18,2)) as imposto
            , cast(freight as numeric(18,2)) as frete
            , cast(totaldue as numeric(18,2)) as total_devido_por_pedido    --total_devido_por_pedido = subtotal_devido + imposto + frete [TOTAL DO PEDIDO]
            --revisionnumber,
            --duedate,
            --accountnumber,
            --billtoaddressid,
            --creditcardapprovalcode,
            --currencyrateid,
            --comment,
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed