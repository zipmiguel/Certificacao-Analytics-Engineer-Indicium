with 
    source as (
        select *
        from {{ source('erp', 'production_product') }}
    )
    , renamed as (
        select
            cast(productid as int) as pk_produto
            , cast(productsubcategoryid as int) as fk_subcategoria_produto
            , cast(name as varchar) as nome_produto            
            , cast(productnumber as varchar) as numero_produto
            , cast(safetystocklevel as int) as nivel_de_estoque_de_seguranca
            , cast(reorderpoint as int) as ponto_de_reabastecimento
            , cast(daystomanufacture as int) as dias_para_fabricar            
            , sellstartdate::date as data_inicio_vendas --Converte sellstartdate de timestamp (data e hora) para date (apenas data)
            --makeflag,
            --finishedgoodsflag,
            --color,   
            --standardcost,
            --listprice,
            --size,
            --sizeunitmeasurecode,
            --weightunitmeasurecode,
            --weight,
            --productline,
            --class,
            --style,            
            --productmodelid,            
            --sellenddate,
            --discontinueddate,
            --rowguid,
            --modifieddate
        from source
    )
select *
from renamed