with 
    source as (
        select * 
        from {{ source('erp', 'salesorderheader') }}
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
            , cast(onlineorderflag as boolean) as sinalizador_pedido_online --Se for TRUE o pedido foi no site, se for FALSE foi em uma loja (store)
            , cast(purchaseordernumber as varchar) as codigo_pedido
            , cast(subtotal as numeric(18,2)) as subtotal_devido
            , cast(taxamt as numeric(18,2)) as imposto
            , cast(freight as numeric(18,2)) as frete
            , cast(totaldue as numeric(18,2)) as total_devido
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