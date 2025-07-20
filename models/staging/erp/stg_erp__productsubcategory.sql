with 
    source as (
        select * 
        from {{ source('erp', 'productsubcategory') }}
    )
    , renamed as (
        select
            cast(productsubcategoryid as int) as pk_subcategoria_produto
            , cast(productcategoryid as int) as fk_categoria_produto            
            , cast(name as varchar) as nome_subcategoria_produto
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed