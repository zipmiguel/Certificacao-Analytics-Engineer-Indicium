# 🚀 Projeto Adventure Works - Certificação em Engenharia de Analytics by Indicium

## 🎯 Objetivo
Desenvolver uma solução de analytics para a Adventure Works, iniciando pela área de vendas, com foco em modelagem dimensional, transformação e validação dos dados brutos. O objetivo é garantir qualidade dos dados e gerar insights estratégicos por meio de dashboards interativos.

## 📌 Etapas do Projeto

### Modelagem Conceitual
Representa a modelagem dimensional com tabelas fato e dimensão, mapeando as tabelas de origem buscando respondender as perguntas de negócio. 

### Armazenamento dos Dados
Utilização do Snowflake como Data Warehouse em nuvem.

### Transformação com dbt Cloud
Aplicação de boas práticas como:

- Camadas: staging, intermediate e marts
- Testes de qualidade: unicidade, nulos e relacionamentos
- Documentação automatizada
- Versionamento via GitHub
- Teste solicitado pelo CEO Carlos Silveira para garantir que o faturamento bruto em 2011 seja exatamente $12.646.112,16, conforme auditoria

### Visualização com Power BI
Desenvolvimento de um dashboard interativo com análise de KPIs, clientes, localidades e produtos.

## 📊 Entregas

### 📌 Modelo Conceitual (Diagrama)
🔗 [Visualizar Diagrama](https://drive.google.com/file/d/1j32PX43NWC7F_HcWlK5Bo4JowJDS6MRR/view?usp=sharing)

### 📈 Dashboard Power BI
Com três abas, todas com filtros por ano, mês, produto, motivo de venda, localização, cliente, tipo de cartão e status:

#### Visão Geral
- KPIs: faturamento bruto, produtos vendidos, pedidos efetivados e ticket médio por pedido
- Gráfico de linha com evolução temporal de faturamento, pedidos efetivados e produtos vendidos
- Tabela resumo de vendas por produto e por cliente, com informações de produtos vendidos, pedido efetivados e faturamento bruto

#### Clientes e Localidades
- KPIs: faturamento bruto, número de cidades com vendas, cidade líder em vendas e ticket médio por cliente
- Gráfico com de linha com evolução temporal de faturamento por país
- Gráficos de barra empilhada com top 10 clientes por faturamento
- Gráficos de barra empilhada com top 5 cidades por faturamento

#### Análise de Produtos
- KPIs: faturamento bruto, produtos vendidos, produto líder em faturamento e ticket médio por produto
- Gráfico com de linha com evolução temporal dos produtos maior faturamento
- Gráficos de barra empilhada com ticket médio por produto
- Gráficos de barra empilhada com top 5 produtos mais vendidos

🔗 [Acessar Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNDE4ZTNjOTgtNjFmMy00ZTBkLTgxYjItNmU5MGVlMTFjYjRhIiwidCI6ImZhNzk1MzFjLThjZTUtNGJkMy05N2VlLTI0NWU2ZWUyNjZiOCJ9)

### 🎥 Vídeo Explicativo
Apresentação completa do projeto: data warehouse na nuvem com Snowflake, modelagem e transformação com dbt, e visualização com Power BI.  
🔗 [Assistir no YouTube](https://www.youtube.com/watch?v=SYeKGkhWh3k)

## ⚙️ Ferramentas Utilizadas

| Ferramenta | Descrição                                           |
|------------|-----------------------------------------------------|
| draw.io    | Criação do diagrama conceitual (modelo dimensional) |
| Snowflake  | Data Warehouse em nuvem                             |
| dbt Cloud  | Transformação, modelagem e testes de dados          |
| GitHub     | Versionamento de código                             |
| Power BI   | Desenvolvimento de dashboard interativo             |

## 👨‍💻 Autor
**Miguel Philippi Nobre**  
Project Analyst | SQL | Data Analysis | dbt | Snowflake | Power BI  
🔗 [LinkedIn](https://www.linkedin.com/in/miguel-philippi/)
