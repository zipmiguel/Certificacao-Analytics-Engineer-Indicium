-- teste solicitado pelo CEO Carlos Silveira para garantir que o valor de vendas brutas em 2011, segundo a auditoria, seja exatamente 12.646.112,16

with vendas_totais_2011 as (
    select
        round(sum(valor_total_bruto), 2) as total_vendas_brutas
    from {{ ref('int_detalhes_vendas') }}
    where extract(year from data_pedido) = 2011
)

select total_vendas_brutas
from vendas_totais_2011
where total_vendas_brutas != 12646112.16