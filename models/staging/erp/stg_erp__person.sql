with 
    source as (
        select * 
        from {{ source('erp', 'person') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as pk_pessoa            
            , case
              when persontype = 'SC' then 'Contato Loja' 
                when persontype = 'IN' then 'Cliente Individual'
                when persontype = 'SP' then 'Vendedor'
                when persontype = 'EM' then 'Funcionario (nao_vendas)'
                when persontype = 'VC' then 'Fornecedor'
                when persontype = 'GC' then 'Contato Geral'
                else null
            end as tipo_de_pessoa
            -- Concatena firstname, middlename (com espaço) e lastname, usando COALESCE para substituir valores NULL por strings vazias e evitar espaços extras; trim() remove espaços no início e no fim do resultado
            , trim(
                COALESCE(firstname, '') || ' ' ||
                COALESCE(middlename || ' ', '') ||
                COALESCE(lastname, '')
            ) as nome_completo          
            --namestyle,
            --title,
            --suffix,
            --emailpromotion,
            --additionalcontactinfo,
            --demographics,
            --rowguid,
            --modifieddate
        from source
    )
select * 
from renamed