# Squad 1 · Dupla 3 — Data Quality em Tempo Real

Pipeline de Data Quality para `ecommerce_produtos` e `ecommerce_categorias`, parte do estágio EmpregaDados.

## Fluxo
ADLS Gen2 (parquet, snapshots em tempo real) → Databricks (Spark, autenticação OAuth via `.options()`) → camadas Bronze e Silver em **Delta**, gravadas por caminho dentro do container `squad1` do ADLS (não tabela gerenciada por catálogo — cada conta Free Edition tem Unity Catalog isolado, então esse é o único formato que a squad inteira consegue enxergar).

## Notebooks
- **`bronze_produtos_categorias`** — lê os micro-lotes do `raw`, sem transformação, adiciona `bronze_ingested_at` e `bronze_source_file`, grava em `squad1/bronze/` particionado por data, idempotente por arquivo.
- **`silver_produtos_categorias`** — aplica as 20 regras oficiais (técnica + negócio) como coluna booleana por linha (`regra_01_...` a `regra_10_...`), grava em `squad1/silver/` em append.

## Como rodar
Requer um `.env` na mesma pasta (não versionado) com as credenciais do ADLS. Rodar célula por célula, na ordem: Bronze primeiro, Silver depois.

## Achados principais
- **Autenticação:** `spark.conf.set()` global e RDD (`sparkContext.parallelize`) não funcionam no Serverless. Solução: credenciais passadas por chamada via `.options(**adls_options)`.
- **Unity Catalog é isolado por conta** no Free Edition — tabela "compartilhada" só existe como Delta por caminho no ADLS, não como tabela de catálogo.
- **Qualidade dos dados:** `preco_lista <= 0` em 1,4% dos produtos (proposital, confirmado pelo Geovany); subcategorias sem produto associado em observação.
- **Regra de caractere inválido em `nome_categoria`:** o `&` foi liberado pelo Geovany — categorias como "Adega & Destilados" são nomes legítimos, não dado sujo.
- **Duas regras com dependência externa, sem inventar valor:** venda nos últimos 90 dias (dados de outra dupla — liberado usar a Bronze deles provisoriamente, trocar por Silver quando publicarem) e contagem de categorias raiz contra `config.py` (valor de referência ainda não informado).

## Pendente
- Confirmar destino de `squad1/dq_monitoring_logs` (tabela de log compartilhada pela squad — caminho exato ainda não definido).
- Consolidar nomenclatura de coluna de regra entre as duplas da squad, hoje não padronizada.

Documentação completa no Notion da squad.