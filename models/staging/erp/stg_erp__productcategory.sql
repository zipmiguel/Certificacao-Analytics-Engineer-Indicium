with 
    source as (
        select * 
        from {{ source('erp', 'production_productcategory') }}
    )
    , renamed as (
        select
            cast(productcategoryid as int) as pk_categoria_produto
            , cast(name as varchar) as nome_categoria_produto
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed