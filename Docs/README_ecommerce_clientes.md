Squad 1 — Qualidade de Dados em Tempo Real

Projeto de Engenharia de Dados — Ecommerce Clientes

Dupla: Emilly e Cleber

## Objetivo

Desenvolver um pipeline de engenharia de dados para ingestão, tratamento, validação e monitoramento da qualidade dos dados de clientes de um e-commerce.

## Tecnologias

* Azure Data Lake Storage Gen2
* Databricks
* Python e PySpark
* Delta Lake
* GitHub

## Arquitetura

Raw → Bronze → Silver → Gold

## Bronze

Realiza a ingestão incremental dos arquivos Parquet, adiciona metadados e armazena os dados em formato Delta.

## Silver

Realiza o tratamento dos dados, a deduplicação e a aplicação das regras de qualidade da tabela ecommerce_clientes.

## Gold

Disponibiliza indicadores de qualidade para análise e visualização em BI.

Regra 8: monitoramento de clientes com e sem pedidos associados.

Regra 9: monitoramento de provedores de e-mail reconhecidos, considerando a meta mínima de 95%.

## Monitoramento

O notebook dq_monitoring_logs.ipynb registra os resultados das verificações de qualidade, incluindo status, severidade e quantidade de registros com falhas.

Processamento incremental

O pipeline utiliza polling para identificar novos arquivos e grava os resultados em Delta Lake com modo append, evitando reprocessar arquivos já registrados.

Notebooks

* Bronze_ecommerce_clientes.ipynb
* Silver_ecommerce_clientes.ipynb
* Gold_ecommerce_clientes.ipynb
* dq_monitoring_logs.ipynb

## Resultado

Pipeline de qualidade de dados implementado com validações na Silver, indicadores na Gold e monitoramento das regras de qualidade.
