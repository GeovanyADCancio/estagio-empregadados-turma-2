# Squad 1 · Dupla 3 — Data Quality em Tempo Real

Pipeline de Data Quality para `ecommerce_produtos` e `ecommerce_categorias`, parte do estágio EmpregaDados.

## Fluxo
ADLS Gen2 (parquet, snapshots em tempo real) → Databricks (Spark, autenticação OAuth via `.options()`) → SQL Server (Azure), com 20 regras de qualidade validadas.

## Notebooks
- **`03_conexao_adls_rafael`** — conexão com o ADLS, leitura dos snapshots, EDA e gravação inicial das tabelas.
- **`04_regras_dq_rafael`** — as 20 regras técnicas e de negócio (planilha oficial de 26/09), gravadas em formato vertical de métricas.

## Como rodar
Requer um `.env` na mesma pasta (não versionado) com as credenciais do ADLS e do SQL Server. Rodar célula por célula, na ordem.

## Achados principais
- **Autenticação:** `spark.conf.set()` e RDD (`sparkContext.parallelize`) não funcionam no Serverless. Solução: `spark.read.format("parquet").options(**adls_options).load(...)`, credenciais passadas por chamada.
- **Qualidade dos dados:** 1,4% dos produtos com `preco_lista <= 0` (proposital, confirmado pelo Geovany); 18,5% das categorias com caractere inválido no nome; 17% das subcategorias sem produto associado.
- **Duas regras bloqueadas** por dependência externa (venda nos últimos 90 dias; contagem de referência de categorias raiz), registradas sem inventar valor.

## Pendente
Confirmar se a camada Silver final vive em formato Delta (Unity Catalog) ou SQL Server — ver documentação completa no Notion da squad.
