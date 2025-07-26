with 
    source as (
        select * 
        from {{ source('erp', 'sales_salesorderdetail') }}
    )
    , renamed as (
        select            
            cast(salesorderdetailid as int) as pk_detalhes_pedido
            , cast(salesorderid as int) as fk_pedido
            , cast(productid as int) as fk_produto
            , cast(orderqty as int) as quantidade_pedido
            , cast(unitprice as numeric(18,2)) as preco_unitario
            , cast(unitpricediscount as numeric(18,2)) as desconto_preco_unitario
            --carriertrackingnumber        
            --specialofferid            
            --rowguid
            --modifieddate
        from source
    )
select * 
from renamed
