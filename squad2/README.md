# 🚀 Squad 2 — Real Time for Business
## Dupla 1: Lucas Sousa Santos Oliveira & Zaiden Emiliano Segundo Seleme

Documentação técnica oficial e artefatos de entrega da **Squad 2 — Dupla 1** para o programa de estágio em Engenharia de Dados (Turma 2).

---

### 📌 1. Escopo e Governança
* **Squad:** 2 — Real Time for Business (Streaming em Tempo Real para Análise de Negócio)
* **Dupla 1:** Lucas Sousa Santos Oliveira & Zaiden Emiliano Segundo Seleme
* **Branch Git:** `feat/squad2-lucas_zaiden`
* **Liderança Técnica (Tech Lead):** Geovany Aparecido Duarte Câncio
* **Scrum Master:** Patrick Marangoni Neri da Silva
* **Product Owner:** Vinícius de Souza Silveira

#### Tabelas sob nossa responsabilidade:
| Tabela | Formato de Origem | Chave Primária (PK) | Chave Estrangeira (FK) | Volumetria de Tempo Real |
| :--- | :--- | :--- | :--- | :--- |
| **`ecommerce_produtos`** | Parquet (`.parquet`) | `sku` | `id_categoria` | 4.800 linhas brutas $\rightarrow$ **2.874 SKUs únicos** |
| **`ecommerce_categorias`** | Parquet (`.parquet`) | `id_categoria` | `id_categoria_pai` | 810 linhas brutas $\rightarrow$ **135 Categorias únicas** |

* **Origem dos Dados (ADLS Gen2):**
  * Storage Account: `internshipdatalake`
  * Container: `raw`
  * Particionamento temporal: `real-time-data/YYYY/MM/DD/HHMMSS/*.parquet`
* **Destino no Azure SQL Server:**
  * Host: `srv-database-intership.database.windows.net`
  * Database: `internshipDatabase`
  * Schema da Squad: `squad2`
  * Tabelas finais: `squad2.ecommerce_produtos` e `squad2.ecommerce_categorias`
  * Modo de escrita: `overwrite`

---

### 🏗️ 2. Arquitetura da Solução e Boas Práticas de Big Data

```
┌──────────────────────────────────────┐
│   Azure Data Lake Storage Gen2       │
│   raw/real-time-data/*/*/*/*/*.parquet
└──────────────────┬───────────────────┘
                   │
                   │ (1) Leitura Paralela Distribuída via abfss://
                   │     Autenticação Granular OAuth (.options)
                   ▼
┌────────────────────────────────────────────────────────┐
│             Databricks Serverless Compute              │
│                                                        │
│  [Nós Executores / Worker Nodes do Cluster Spark]     │
│  • Leitura concorrente das janelas Parquet             │
│  • Deduplicação distribuída: dropDuplicates(["sku"])   │
│  • Transformação e validação de tipos de dados         │
└──────────────────┬─────────────────────────────────────┘
                   │
                   │ (2) Carga DML Distribuída
                   │     Conector Nativo format("sqlserver")
                   ▼
┌──────────────────────────────────────┐
│          Azure SQL Server            │
│   Schema: squad2                     │
│   • squad2.ecommerce_produtos        │
│   • squad2.ecommerce_categorias      │
└──────────────────┬───────────────────┘
                   │
                   │ (3) Auditoria de Data Quality Pós-Carga
                   │     Catalyst Optimizer (0 nulos, 0 órfãos)
                   ▼
┌──────────────────────────────────────┐
│     Looker / BI & Visualização       │
│     (Camada de Consumo de Negócio)   │
└──────────────────────────────────────┘
```

#### ⚡ Destaques Técnicos de Engenharia:
1. **Processamento 100% Distribuído (Cluster Spark vs. Driver):**
   * A ingestão NÃO utiliza o nó Driver para download manual com Pandas (evitando gargalos de memória e riscos de *Out of Memory*).
   * A leitura ocorre diretamente nos **executores do cluster Spark** através do conector nativo `abfss://`.
2. **Autenticação Granular por Chamada (`.options(**adls_options)`):**
   * As credenciais do Service Principal são injetadas diretamente na chamada do `spark.read.options(...)`, contornando a restrição de alteração global no Databricks Serverless / Free Edition de forma homologada.
3. **Conector Nativo SQL Server para DML:**
   * Utilização de `format("sqlserver")` conforme exigência da arquitetura Serverless do Databricks (que bloqueia o conector genérico JDBC para escrita).

---

### 🚀 3. Ordem Oficial de Execução dos Notebooks

#### 📦 Sprint 1 (Histórico e Carga Inicial no SQL Server):
```text
00_setup_config.ipynb ──> 01_extracao_produtos_categorias.ipynb ──> 02_carga_sqlserver.ipynb ──> 03_auditoria_data_quality.ipynb
```
* **Passo 1:** `00_setup_config.ipynb` — Configuração dinâmica de `.env` e dependências.
* **Passo 2:** `01_extracao_produtos_categorias.ipynb` — Extração distribuída do ADLS Gen2 (`raw/real-time-data/`).
* **Passo 3:** `02_carga_sqlserver.ipynb` — Carga no Azure SQL Server (`format("sqlserver")`).
* **Passo 4:** `03_auditoria_data_quality.ipynb` — Auditoria pós-carga via Catalyst (0 nulos, 0 órfãos).

#### 🏅 Sprint 2 (Arquitetura Medalhão Delta Lake - Bronze & Silver):
```text
04_bronze_ingestao_delta.ipynb ──> 05_silver_limpeza_tratamento.ipynb
```

* **Passo 5:** `04_bronze_ingestao_delta.ipynb` (Task 1 — Ingestão Bronze com Metadados)
  * Leitura incremental do bucket `raw` via **Auto Loader (`cloudFiles`)** sob demanda (`.trigger(availableNow=True)`).
  * Adição obrigatória dos metadados de auditoria: `bronze_ingested_at` e `bronze_source_file`.
  * **Zero transformação:** preservação integral do dado bruto.
  * Particionamento físico temporal por `ano`, `mes`, `dia`, `hora`.
  * Gravação em Delta em modo `append` no namespace isolado `squad2/grupo1/bronze/`.
  * Registro e auditoria de cada lote na tabela de controle Delta `squad2.ingestion_control_log`.

* **Passo 6:** `05_silver_limpeza_tratamento.ipynb` (Task 2 — Ingestão Silver, Limpeza e Quarentena)
  * Consumo incremental da Bronze via Delta Streaming (`trigger(availableNow=True)`).
  * Injeção do metadado de auditoria: `silver_processed_at`.
  * Limpeza de strings (`trim`) e casting estrito de tipos (`preco_lista`, `is_ativo`).
  * **Validação de Regras Técnicas com Quarentena:**
    * *Produtos:* SKU entre 5 e 60 caracteres, preço entre 0 e 5000, `is_ativo` não nulo.
    * *Categorias:* `id_categoria` e `nome_categoria` obrigatórios.
    * Registros inválidos são segregados em `squad2/grupo1/quarantine/` com `quarantine_reason` e `quarantined_at`.
  * **Alertas de Negócio em Tempo Real:** Alerta para lotes com $> 50$ novos SKUs, alerta crítico para produtos ativos com preço $\le 0$, e monitoramento de categorias raiz.
  * Gravação dos registros válidos em Delta Silver em modo `append` no namespace `squad2/grupo1/silver/`.

---

### 📋 4. Textos para Atualização das Tasks no Trello

#### **Task 1: Setup e Configuração**
> **Configuração inicial do ambiente e versionamento concluídos com sucesso.**
> * Repositório clonado e branch **`feat/squad2-lucas_zaiden`** criada para desenvolvimento isolado da Dupla 1.
> * Módulo de configuração (**`00_setup_config.ipynb`**) refatorado para utilizar `python-dotenv` com busca dinâmica, garantindo o carregamento seguro das credenciais sem injeção global de variáveis (evitando bloqueios de arquitetura no Databricks Serverless).

#### **Task 2: Extração Distribuída no Data Lake**
> **Conexão e extração distribuída no cluster Spark estabelecidas com sucesso.**
> * Injeção granular das credenciais OAuth (Service Principal) diretamente no `spark.read.options(...)`, contornando a restrição de autenticação global do Databricks Serverless sem sobrecarregar o nó driver.
> * Leitura paralelizada e distribuída nos executores do cluster Spark através do protocolo `abfss://` a partir de `raw/real-time-data/*/*/*/*/*.parquet`.
> * Consolidação e deduplicação nativa em PySpark (`dropDuplicates`), garantindo escalabilidade para Big Data.
> * **DataFrames consolidados:** `df_produtos` (**2.874 SKUs únicos**) e `df_categorias` (**135 categorias únicas**).

#### **Task 3: Engenharia Reversa e Auditoria de Data Quality**
> **Engenharia reversa e análise exploratória (Data Quality) concluídas com sucesso.**
> * Dados lidos diretamente do banco SQL Server para o Databricks após a carga (notebook **`03_auditoria_data_quality.ipynb`**).
> * Validações de integridade realizadas:
>   * Estrutura (`printSchema`) e amostragem visual (`display`).
>   * Contagem de volume: 2.874 produtos e 135 categorias confirmados no banco.
>   * Auditoria de chave primária: **0 nulos** em `sku` e `id_categoria`.
>   * Integridade referencial: 100% dos produtos associados a categorias válidas.
> * Tabelas validadas: **`squad2.ecommerce_produtos`** e **`squad2.ecommerce_categorias`**.

#### **Task 4: Pipeline de Carga no Azure SQL Server**
> **Pipeline de carga (Data Load) finalizado com sucesso no Azure SQL Server.**
> * Carga realizada no notebook **`02_carga_sqlserver.ipynb`** via conector nativo `format("sqlserver")` homologado pelo Databricks Serverless.
> * A gravação consolidou as janelas temporais na estrutura definitiva do banco no schema da squad.
> * **Tabelas destino:** `squad2.ecommerce_produtos` e `squad2.ecommerce_categorias`.
> * **Modo de escrita:** `overwrite`.
> * **Status da Conexão e Carga:** Sucesso.

---

### 🔒 5. Segurança e Proteção de Dados
1. **Nenhuma credencial versionada:** As credenciais do ADLS Gen2 e do Azure SQL Server residem exclusivamente no arquivo `.env`.
2. **Proteção no `.gitignore`:** O `.gitignore` garante que arquivos `.env` jamais sejam rastreados ou comitados no repositório compartilhado.
3. **Isolamento de Squad:** Manipulação restrita ao schema `squad2` e container `squad2`, respeitando as regras de integridade do ambiente multi-tenant.
