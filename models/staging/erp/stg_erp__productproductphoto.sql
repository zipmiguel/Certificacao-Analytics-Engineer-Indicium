with 
    source as (
        select * from {{ source('erp', 'productproductphoto') }}
    )
    , renamed as (
        select
            cast(productid as int) as fk_produto,
            --productphotoid,
            --primary,
            --modifieddate
        from source
    )
select * 
from renamed