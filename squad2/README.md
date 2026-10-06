# 🚀 Squad 2 — Real Time for Business (Esteira Unificada)
## Dupla 1 & Dupla 5 (Grupo 1 & Grupo 5 Integrados)

**Integrantes:** Zaiden Emiliano Segundo Seleme & Lucas Sousa Santos Oliveira  
**Branch Git Unificada:** `feat/squad2-dupla1e5`  
**Liderança Técnica (Tech Lead):** Geovany Aparecido Duarte Câncio  
**Scrum Master:** Patrick Marangoni Neri da Silva  
**Product Owner:** Vinícius de Souza Silveira  

---

### 📌 1. Escopo e Governança
A **Squad 2 — Real Time for Business** é responsável pelo processamento distribuído em streaming/micro-lotes sobre o **Azure Data Lake Storage Gen2 (ADLS Gen2)** e o **Azure SQL Server**.

Esta branch unifica integralmente as 4 tabelas de tempo real, persistindo as Delta Tables diretamente na **raiz do contêiner `squad2`** (eliminando a segregação legada por subpastas `/grupo1/` e `/grupo5/`):
* `ecommerce_produtos` (Catálogo de Produtos)
* `ecommerce_categorias` (Taxonomia e Árvore de Categorias)
* `ecommerce_rastreamento_entregas` (Logística de Entregas)
* `ecommerce_enderecos` (Inteligência Geográfica de Clientes)

#### Matriz Oficial de Tabelas da Squad 2:
| Tabela | Formato de Origem | Chave Primária (PK) | Chave Estrangeira (FK) | Volumetria Tempo Real |
| :--- | :--- | :--- | :--- | :--- |
| **`ecommerce_produtos`** | Parquet (`.parquet`) | `sku` | `id_categoria` | 4.800 linhas brutas $\rightarrow$ **2.874 SKUs únicos** |
| **`ecommerce_categorias`** | Parquet (`.parquet`) | `id_categoria` | `id_categoria_pai` | 810 linhas brutas $\rightarrow$ **135 Categorias únicas** |
| **`ecommerce_rastreamento_entregas`** | Parquet (`.parquet`) | `id_rastreamento` | `id_pedido_ecommerce` | 3.600 linhas brutas $\rightarrow$ **3.600 Eventos únicos** |
| **`ecommerce_enderecos`** | Parquet (`.parquet`) | `id_endereco` | `id_cliente` | 4.200 linhas brutas $\rightarrow$ **4.200 Endereços únicos** |

---

### 🏛️ 2. Arquitetura Medallion Ponta a Ponta (Raiz do Contêiner `squad2`)

```mermaid
flowchart TD
    subgraph RAW ["Landing Zone (Container raw)"]
        R1["ecommerce_produtos.parquet"]
        R2["ecommerce_categorias.parquet"]
        R3["ecommerce_rastreamento.parquet"]
        R4["ecommerce_enderecos.parquet"]
    end

    subgraph BRONZE ["Camada Bronze (Raiz de squad2: append-only)"]
        B1["squad2/bronze/ecommerce_produtos"]
        B2["squad2/bronze/ecommerce_categorias"]
        B3["squad2/bronze/ecommerce_rastreamento_entregas"]
        B4["squad2/bronze/ecommerce_enderecos"]
        B_CTRL["squad2/metadata/_ingestion_control_log"]
    end

    subgraph SILVER ["Camada Silver (Raiz de squad2: UPSERT)"]
        S1["squad2/silver/ecommerce_produtos"]
        S2["squad2/silver/ecommerce_categorias"]
        S3["squad2/silver/ecommerce_rastreamento_entregas"]
        S4["squad2/silver/ecommerce_enderecos"]
        Q["squad2/quarantine/* (Erros e Violações Técnicas)"]
    end

    subgraph GOLD ["Camada Gold (Raiz de squad2: Modo APPEND)"]
        G1["squad2/gold/gold_dim_produtos"]
        G2["squad2/gold/gold_metricas_categorias"]
        G3["squad2/gold/gold_logistica_pedidos_rota"]
        G4["squad2/gold/gold_performance_transportadoras"]
        G5["squad2/gold/gold_distribuicao_geografica_clientes"]
    end

    subgraph DESTINOS ["Destino Duplo (Dual Sink) - Modo APPEND"]
        D_LAKE["ADLS Gen2 (Delta Lake ACID na raiz de squad2)"]
        D_SQL["Azure SQL Server (Schema squad2)"]
    end

    RAW --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> D_LAKE
    GOLD --> D_SQL
```

#### Estratégias de Persistência Definidas em Reunião Técnica:
* **Padronização na Raiz do Contêiner `squad2`:** Todas as camadas residem em `abfss://squad2@{storage_account}.dfs.core.windows.net/` sob pastas estruturais limpas (`/bronze/`, `/silver/`, `/quarantine/`, `/gold/`, `/metadata/`).
* **Camada Silver (UPSERT):**
  * Utiliza deduplicação por chave primária de negócio (`sku`, `id_categoria`, `id_rastreamento`, `id_endereco`) e upsert atômico ACID via anti-join broadcast + `unionByName` + `overwrite`, eliminando duplicações e garantindo idempotência irrestrita.
* **Camada Gold (Modo APPEND):**
  * Persistência em **modo `append`** no **Destino Duplo** (Delta Lake e Azure SQL Server).
  * Cada execução registra novos snapshots e agregações analíticas carimbadas com `gold_processed_at`, viabilizando a **historização temporal de métricas e KPIs** para análise de evolução em relatórios e dashboards.

---

### 📁 3. Resumo da Esteira Medallion da Squad 2

A esteira da Squad 2 é orquestrada através de 7 notebooks modulares com execução sequencial e proteção defensiva de ponta a ponta:

```mermaid
flowchart TD
    RAW["Raw Container (real-time-data)"] -->|"01 / 04 Ingestão Incremental"| BRONZE["Bronze Layer (Delta Lake /squad2/bronze)"]
    BRONZE -->|"05 Limpeza & Regras de Negócio"| QUARANTINE["Quarentena (Delta Lake /squad2/quarantine)"]
    BRONZE -->|"05 Upsert Atômico ACID"| SILVER["Silver Layer (Delta Lake /squad2/silver)"]
    SILVER -->|"06 Agregações & Data Marts"| GOLD_LAKE["Gold Layer (Delta Lake /squad2/gold)"]
    SILVER -->|"06 Dual-Sink (Modo APPEND)"| SQL_DB["Azure SQL Server (schema squad2)"]
```

#### Detalhamento Funcional dos 7 Notebooks:

1. **`00_setup_config.ipynb` (Setup & Conectividade):**
   * Configuração das credenciais seguras do Service Principal (Microsoft Entra ID) e banco relacional via `.env` dinâmico (`find_dotenv()`).
   * Injeção de credenciais OAuth Spark FQDN (`adls_options`) e validação eager de portas de rede (ADLS Gen2 porta 443 e SQL Server porta 1433).

2. **`01_extracao_distribuida_eda.ipynb` (Extração & EDA):**
   * Leitura resiliente de arquivos brutos via PyArrow, contornando limitações de tipos `TIMESTAMP_NANOS` e inteiros nulos no Databricks Serverless.
   * Análise Exploratória de Dados (EDA) unificada com estatísticas descritivas, cardinalidade e detecção de anomalias preliminares.

3. **`02_carga_sqlserver.ipynb` (Carga Relacional Inicial):**
   * Persistência inicial das 4 tabelas no catálogo `squad2` do Azure SQL Server.
   * **Proteção contra truncamento acidental:** se o lote bruto estiver vazio, a operação é abortada com segurança preservando o banco.
   * Suporte nativo ao conector `format("sqlserver")` com contingência via `pyodbc` (`fast_executemany`).

4. **`03_auditoria_data_quality.ipynb` (Auditoria de Qualidade SQL):**
   * Auditoria de integridade relacional diretamente no banco via conector nativo `format("sqlserver")`.
   * Verificação de 100% de unicidade e zero nulos nas chaves primárias (`sku`, `id_categoria`, `id_rastreamento`, `id_endereco`).
   * Validação de integridade referencial entre produtos e categorias (zero órfãos).

5. **`04_bronze_ingestao_delta.ipynb` (Ingestão Incremental Bronze):**
   * Ingestão incremental append-only particionada por dia (`data_particao`) diretamente na raiz de `squad2/bronze/`.
   * **Watermark de Arquivos Vazios no Control Log:** arquivos com 0 linhas são registrados no `_ingestion_control_log` com `quantidade_linhas = 0`. Nas execuções seguintes, o anti-join os ignora, eliminando custos de I/O desnecessários.
   * Metadados técnicos anexados: `bronze_source_file` e `bronze_ingested_at`.

6. **`05_silver_limpeza_tratamento.ipynb` (Curadoria Silver & Quarentena):**
   * Aplicação rigorosa das regras de Data Quality e negócio.
   * Registros reprovados são desviados para `squad2/quarantine/` com justificativa técnica.
   * **Upsert Atômico ACID:** deduplicação por chave de negócio e snapshot atômico via broadcast anti-join + `unionByName` + `overwrite`, garantindo zero duplicidades e preservando o histórico.

7. **`06_gold_metricas_analiticas.ipynb` (Data Marts & Destino Duplo):**
   * Construção de **5 Data Marts Analíticos** de alto valor para BI e inteligência de negócios.
   * **Destino Duplo (Dual-Sink):** gravação simultânea no Delta Lake e no Azure SQL Server em **modo `append`** com historização por `gold_processed_at`.
   * **Resiliência Automática de Schema:** sincronização dinâmica entre o Lake e a DDL oficial do SQL Server caso haja divergência de tipos legados.

---

### 🛡️ 4. Regras Técnicas, de Negócio e Quarentena

| Entidade | Regra | Tipo | Ação em Falha |
| :--- | :--- | :---: | :--- |
| `ecommerce_produtos` | $5 < \text{len}(sku) < 60$ e não-nulo | Técnica 1 | Quarentena |
| `ecommerce_produtos` | $0 < preco\_lista < 5000$ e não-nulo | Técnica 2 | Quarentena |
| `ecommerce_produtos` | `is_ativo` booleano e não-nulo | Técnica 3 | Quarentena |
| `ecommerce_produtos` | $> 50$ novos SKUs no micro-lote | Negócio 4 | Alerta de Expansão de Catálogo |
| `ecommerce_produtos` | `is_ativo == True` com `preco_lista <= 0` | Negócio 5 | Alerta Crítico Operacional |
| `ecommerce_categorias` | `id_categoria` e `nome_categoria` obrigatórios | Técnica 1 | Quarentena |
| `ecommerce_categorias` | Alteração na contagem de categorias raiz | Negócio 2 | Alerta de Governança |
| `ecommerce_rastreamento`| Chaves obrigatórias preenchidas | Técnica 1 | Quarentena |
| `ecommerce_rastreamento`| `status_entrega` no catálogo oficial (9 status) | Técnica 2 | Quarentena |
| `ecommerce_rastreamento`| `dt_evento <= current_timestamp()` | Técnica 3 | Quarentena |
| `ecommerce_enderecos` | Chaves obrigatórias preenchidas | Técnica 1 | Quarentena |
| `ecommerce_enderecos` | UF válida (pertencente às 27 UFs) | Técnica 2 | Quarentena |
| `ecommerce_enderecos` | CEP numérico com exatamente 8 dígitos | Técnica 3 | Quarentena |

---

### 📊 5. Data Marts Analíticos da Camada Gold (Modo Append)

1. **`gold_dim_produtos`:** Dimensão desnormalizada com categorização hierárquica (categoria pai/filha), faixas mercadológicas de precificação e flag de disponibilidade.
2. **`gold_metricas_categorias`:** KPIs executivos por categoria (volumetria ativo/inativo, taxa de disponibilidade, ticket médio/mínimo/máximo e amplitude de preços).
3. **`gold_logistica_pedidos_rota`:** Throughput logístico diário (volumes coletados, em trânsito, em rota, entregues, falhas operacionais e taxa de sucesso percentual).
4. **`gold_performance_transportadoras`:** Scorecard diário e ranking por volume e eficiência operacional de parceiros de entrega (`dense_rank()`).
5. **`gold_distribuicao_geografica_clientes`:** Inteligência geográfica com penetração de clientes e densidade de endereços por estado (UF).

---

### ⚙️ 6. Resiliência e Soluções Técnicas para Databricks Serverless

* **Leitor Resiliente Parquet:** Contorna incompatibilidades de inferência com `TIMESTAMP_NANOS` e mismatch `INT32 Null` no Databricks Serverless através de conversão PyArrow.
* **Injeção OAuth FQDN:** Configuração das credenciais do Service Principal diretamente via `.options(**adls_options)` em cada chamada Spark, respeitando o isolamento do Unity Catalog / Serverless.
* **Zero RDD & Zero Global Temp Views:** 100% do pipeline opera sobre DataFrames nativos e Catalyst Optimizer.
* **Gestão Idempotente de Micro-Lotes Vazios:** O `_ingestion_control_log` na Bronze rastreia arquivos sem registros (`quantidade_linhas = 0`), prevenindo releituras em loop.
* **Upsert Atômico ACID na Silver:** Elimina a necessidade de APIs sujeitas a restrições de cluster, realizando o upsert por anti-join broadcast + `unionByName` + `overwrite` atômico.
* **Dual Sink com Resiliência de Schema na Gold:** Gravação cumulativa em modo `append` tanto no Delta Lake quanto no Azure SQL Server, com projeção exata das colunas da DDL oficial.
