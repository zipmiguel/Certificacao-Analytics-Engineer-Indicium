with 
    salesorderdetail as (
        select * 
        from {{ ref('stg_erp__salesorderdetail') }}
    )

    , salesorderheader as (
        select * 
        from {{ ref('stg_erp__salesorderheader') }}
    )

    , joined as (
        select 
            sod.pk_detalhes_pedido                      
            , sod.fk_pedido                               
            , sod.fk_produto                              
            , sod.quantidade_produto                      
            , sod.preco_unitario_produto                          
            , sod.desconto_preco_unitario                

            , soh.pk_pedido                          
            , soh.fk_cliente                               
            , soh.fk_vendedor                              
            , soh.fk_territorio                            
            , soh.fk_endereco_entrega                     
            , soh.fk_cartao_credito                        
            , soh.data_pedido                              
            , soh.data_envio                               
            , soh.status_pedido                            
            , soh.nome_status_pedido                       
            , soh.sinalizador_pedido_online              
            , soh.codigo_pedido                           
            , soh.subtotal_devido_por_pedido                         
            , soh.imposto                                  
            , soh.frete                                    
            , soh.total_devido_por_pedido                             
        from salesorderdetail as sod
        inner join salesorderheader as soh on sod.fk_pedido = soh.pk_pedido
    )

    , calculado as (
        select
            *
            --Valor bruto negociado do item (sem desconto), necessário para cálculo de ticket médio, top produtos e análises por item
            , preco_unitario_produto * quantidade_produto as valor_total_bruto 

            --Valor líquido negociado do item (com desconto), valor total bruto - desconto desconto_preco_unitario, essencial para responder perguntas como total negociado, top clientes, etc.
            , valor_total_bruto * (1 - desconto_preco_unitario) as valor_total_liquido  

            --Distribui o frete igualmente entre contagem de produtos distintos mesmo pedido, permitindo análises por produto, cliente, cidade etc.
            , cast(frete / count(*) over (partition by fk_pedido) as numeric(18,2)) as frete_por_produto 

            --Distribui o imposto igualmente entre contagem de produtos distintos no mesmo pedido, necessário para análises financeiras por linha de produto
            , cast(imposto / count(*) over (partition by fk_pedido) as numeric(18,2)) as imposto_por_produto 
        from joined
    )

    , chave_surrogate as (
        select 
            {{ dbt_utils.generate_surrogate_key(['fk_pedido', 'fk_produto']) }} as sk_detalhes_vendas
            , *
        from calculado
    )

    , ordem_colunas as (
        select
            sk_detalhes_vendas                               
            , pk_detalhes_pedido                       
            , fk_pedido                                
            , fk_produto                               
            , fk_cliente                               
            , fk_vendedor                              
            , fk_territorio                            
            , fk_endereco_entrega                      
            , fk_cartao_credito                        
            , data_pedido                              
            , data_envio                               
            , status_pedido                            
            , nome_status_pedido                       
            , sinalizador_pedido_online               
            , codigo_pedido                           
            , quantidade_produto                       
            , preco_unitario_produto                          
            , desconto_preco_unitario   
            --#Comentados pois só seriam usados para auditar se a somatória de imposto_por_item_do_pedido, frete_por_item_do_pedido corresponderia ao valor de imposto e frete respectivamente, da mesma forma para valor total líquido e bruto em relação ao total_devido_por_pedido                            
            --, subtotal_devido_por_pedido   --subtotal_devido_por_pedido = quantidade_produto * preco_unitario_produto [TOTAL DO PEDIDO]     
            --, imposto                      --imposto total por pedido           
            --, frete                        --frete total por pedido         
            --, total_devido_por_pedido      --total_devido_por_pedido = subtotal_devido + imposto + frete [TOTAL DO PEDIDO]                                    
            , imposto_por_produto     -- imposto_por_produto = imposto / contagem de produtos distintos no pedido, e não por quantidade_produto
            , frete_por_produto       -- frete_por_produto = frete / contagem de produtos distintos no pedido, e não por quantidade_produto
            , valor_total_bruto       -- valor_total_bruto = preco_unitario_produto * quantidade_produto [por produto ou por linha do pedido]
            , valor_total_liquido     -- valor_total_liquido = valor_total_bruto - desconto_preco_unitario [por produto ou por linha do pedido]  
        from chave_surrogate
    )

select *
from ordem_colunas