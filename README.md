# 🚀 Squad 2 - Real Time for Business


## Grupo 3 - Ecommerce Pedidos

---

# 👥 Equipe

Projeto desenvolvido pelo:

**Squad 2 - Real Time for Business**

**Grupo 3 - Ecommerce Pedidos**

Projeto desenvolvido durante estágio de Engenharia de Dados.

---

# 📌 Visão Geral

Este projeto tem como objetivo desenvolver um pipeline de Engenharia de Dados para processamento, tratamento e validação de dados de pedidos de ecommerce.

A solução foi construída utilizando arquitetura **Lakehouse**, seguindo o modelo **Medallion Architecture**, permitindo organizar os dados em camadas de processamento:

- Dados brutos;
- Dados tratados;
- Dados validados;
- Dados preparados para consumo analítico.

O pipeline utiliza:

- Databricks;
- Azure Data Lake Storage Gen2;
- Apache Spark;
- PySpark;
- Delta Lake.

---

# 🎯 Objetivo do Projeto

Construir uma estrutura capaz de receber dados de pedidos de ecommerce, garantindo:

- Organização das informações;
- Rastreabilidade dos dados;
- Qualidade dos registros;
- Separação entre ingestão e transformação;
- Preparação para análises futuras.

---

# 🏗️ Arquitetura do Projeto

O projeto utiliza arquitetura baseada no modelo:

## Medallion Architecture

```text
                    Azure Data Lake Gen2


                           RAW

                            ↓


                      🥉 BRONZE

          Dados ingeridos + rastreabilidade


                            ↓


                  🛡️ DATA QUALITY

           Validações + Controle + Metadata


                            ↓


                      🥈 SILVER

           Dados tratados e padronizados


                            ↓


                      🥇 GOLD

              Indicadores de negócio

              (Próxima etapa)
```

---

# 🧰 Tecnologias Utilizadas

## ☁️ Cloud

- Microsoft Azure
- Azure Data Lake Storage Gen2
- Azure SQL Server

---

## ⚙️ Processamento

- Databricks
- Apache Spark
- PySpark

---

## 💾 Armazenamento

- Delta Lake
- Unity Catalog

---

## 💻 Linguagens

- Python
- SQL

---

# 🔐 Segurança e Configuração

O projeto utiliza variáveis de ambiente através do arquivo:

```text
.env
```

As informações protegidas incluem:

- Client ID;
- Tenant ID;
- Client Secret;
- Configurações de conexão SQL.

As credenciais não ficam expostas diretamente no código-fonte.

---

# 📁 Controle de Versionamento

O projeto utiliza:

```text
.gitignore
```

para impedir o envio de arquivos sensíveis ao repositório.

Exemplo:

```text
.env
```

O arquivo de credenciais permanece apenas no ambiente local.

---

# 📂 Estrutura do Projeto

```text
squad2_ecommerce_real_time

│
├── 00_setup_config.ipynb
│
├── 01_conexao_adls_gen2.ipynb
│
├── 02_eda_ecommerce.ipynb
│
├── 03_ingestao_bronze_pedidos.ipynb
│
├── 04_silver_tratamento_pedidos.ipynb
│
├── 05_data_quality_pedidos.ipynb
│
├── .env
│
├── .gitignore
│
└── README.md
```

---

# 📘 Descrição dos Notebooks

---

# ⚙️ 00_setup_config.ipynb

Responsável pela configuração inicial do projeto.

Atividades:

- Carregamento das variáveis de ambiente;
- Validação das configurações;
- Organização dos parâmetros utilizados no pipeline.

---

# ☁️ 01_conexao_adls_gen2.ipynb

Responsável pela conexão segura entre Databricks e Azure Data Lake Storage Gen2.

Autenticação utilizando:

```text
Service Principal
```

através de:

```text
ClientSecretCredential
```

Cliente utilizado:

```text
DataLakeServiceClient
```

Fluxo:

```text
Databricks

↓

Service Principal

↓

Azure Data Lake Gen2

↓

Container RAW
```

---

# 📊 02_eda_ecommerce.ipynb

Responsável pela análise exploratória dos dados.

Foram avaliados:

- Estrutura dos dados;
- Schema;
- Volume;
- Qualidade;
- Distribuição dos pedidos;
- Métodos de pagamento;
- Status dos pedidos.

---

# 📦 Processamento Batch

Arquivo analisado:

```text
ecommerce_pedidos.csv
```

Resultados:

```text
Registros analisados:

90.335


Duplicidades:

0


Valores nulos:

0
```

---

# ⚡ Processamento Real Time

Foram identificados:

```text
6 micro-lotes
```

Quantidade processada:

```text
27 registros
```

---

# 🥉 03_ingestao_bronze_pedidos.ipynb

Responsável pela construção da camada Bronze.

Objetivo:

Armazenar os dados conforme chegam, mantendo rastreabilidade.

Origem:

```text
RAW
```

Destino:

```text
workspace.grupo3_bronze.ecommerce_pedidos
```

---

## Metadados adicionados

Foram criados:

```text
bronze_source_file

bronze_ingested_at
```

Permitem identificar:

- Origem do arquivo;
- Momento da ingestão.

---

## Resultado Bronze

Formato:

```text
Delta Lake
```

Resultado:

```text
27 registros

12 colunas
```

---

# 🥈 04_silver_tratamento_pedidos.ipynb

Responsável pelo tratamento e padronização dos dados.

Origem:

```text
workspace.grupo3_bronze.ecommerce_pedidos
```

Destino:

```text
workspace.grupo3_silver.ecommerce_pedidos
```

---

# 🧹 Tratamentos Realizados

## 💳 Padronização dos pagamentos

Antes:

```text
pix

boleto

cartão débito

cartão crédito
```

Depois:

```text
Pix

Boleto

Cartão
```

---

## Auditoria Silver

Campo criado:

```text
silver_processed_at
```

---

## Resultado Silver

Formato:

```text
Delta Lake
```

Resultado:

```text
27 registros

13 colunas
```

---

# 🛡️ 05_data_quality_pedidos.ipynb

Responsável pela validação da qualidade dos dados.

Foram aplicadas regras técnicas e de negócio.

---

# ✅ Validações Implementadas

## Schema obrigatório

Campos:

```text
id_pedido

id_cliente

dt_pedido

status_pedido

valor_total

metodo_pagamento
```

Resultado:

```text
✅ Aprovado
```

---

## Valor total

Regra:

```text
valor_total > 0
```

Resultado:

```text
❌ 1 inconsistência encontrada
```

Problema:

```text
valor_total = -1.92
```

---

## Datas

Campos:

```text
dt_pedido

dt_previsao_entrega

dt_ultima_atualizacao_status
```

Resultado:

```text
✅ Aprovado
```

---

## Status do pedido

Resultado:

```text
❌ 2 registros encontrados
```

Valor identificado:

```text
Aguardando
```

---

# 🗂️ Camada Metadata

Criada para controle e governança do pipeline.

Estrutura:

```text
workspace.grupo3_metadata
```

---

## Controle de ingestão

Tabela:

```text
grupo3_metadata.ingestion_control
```

Responsável por:

- Registrar arquivos processados;
- Controlar histórico;
- Evitar reprocessamentos.

Resultado:

```text
6 arquivos registrados
```

---

## Resultado Data Quality

Tabela:

```text
grupo3_metadata.data_quality_results
```

Responsável por:

- Registrar regras executadas;
- Armazenar resultados;
- Criar evidências de qualidade.

---

# 🔄 Fluxo Atual do Pipeline

```text
RAW

↓

🥉 Bronze

↓

🛡️ Data Quality

↓

🥈 Silver

↓

🥇 Gold

(Futura implementação)
```

---

# 🛡️ Boas Práticas Aplicadas

O projeto aplica:

✅ Arquitetura Medallion;

✅ Separação de responsabilidades;

✅ Controle de auditoria;

✅ Proteção de credenciais;

✅ Uso de variáveis de ambiente;

✅ Validação de qualidade;

✅ Controle de ingestão;

✅ Persistência Delta Lake;

✅ Organização por camadas.

---

# 📌 Status do Projeto

## ✅ Concluído

- Configuração do ambiente;
- Conexão Azure Data Lake Gen2;
- Análise exploratória;
- Processamento Batch;
- Processamento Real Time;
- Camada Bronze;
- Camada Silver;
- Data Quality;
- Metadata e controle de ingestão.

---

## 🚀 Próximas etapas

- Camada Gold;
- Indicadores de negócio;
- Dashboards;
- Consumo analítico.

---

# 🏁 Resultado Final

O projeto implementa atualmente um pipeline funcional de Engenharia de Dados utilizando arquitetura Lakehouse.

Fluxo atual:

```text
Azure Data Lake Gen2

↓

Databricks

↓

🥉 Bronze

↓

🛡️ Data Quality

↓

🥈 Silver
```

Os dados foram organizados, tratados, validados e preparados para a futura criação da camada Gold.

---