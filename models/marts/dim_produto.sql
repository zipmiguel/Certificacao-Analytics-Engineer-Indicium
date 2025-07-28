with
    stg_erp__product as (
        select
            pk_produto
            , fk_subcategoria_produto
            , nome_produto             
        from {{ref('stg_erp__product')}}
    )

    , stg_erp__productsubcategory as (
        select
            pk_subcategoria_produto 
            , fk_categoria_produto 
            , nome_subcategoria_produto
        from {{ref('stg_erp__productsubcategory')}}
    )

    , stg_erp__productcategory as (
        select
            pk_categoria_produto 
            , nome_categoria_produto
        from {{ref('stg_erp__productcategory')}}
    )

    , final as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_produto'])}} as sk_produto   
            , stg_erp__product.pk_produto
            , stg_erp__product.nome_produto
            , stg_erp__productcategory.nome_categoria_produto
            , stg_erp__productsubcategory.nome_subcategoria_produto
        from stg_erp__product
        left join stg_erp__productsubcategory on stg_erp__product.fk_subcategoria_produto = stg_erp__productsubcategory.pk_subcategoria_produto
        left join stg_erp__productcategory on stg_erp__productsubcategory.fk_categoria_produto = stg_erp__productcategory.pk_categoria_produto
    )
select *
from final