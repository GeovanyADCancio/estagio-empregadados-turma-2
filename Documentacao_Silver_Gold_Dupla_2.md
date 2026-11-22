# Documentação Técnica — Camadas Silver e Gold

**Projeto:** Real Time for Business  
**Squad:** Squad 2 — Dupla Kauan & Leandro  
**Workspace:** Databricks (AWS, Serverless CPU)  
**Data:** Outubro 2026  

---

## 1. Visão Geral da Arquitetura

```
SQL Server (Azure)
    │
    ▼  Lakehouse Federation
sqlserver_catalog.squad2.ecommerce_clientes    ← Origem Silver
sqlserver_catalog.squad2.ecommerce_pedidos     ← Origem Gold
    │
    ▼  Notebook 05_ingestao_silver_le (Python)
workspace.default.silver_ecommerce_clientes     ← Silver (Delta, MERGE)
workspace.default.quarentena_ecommerce_clientes ← Quarentena (Delta, APPEND)
    │
    ▼  Notebook 06_ingestao_gold_le (SQL + Python)
workspace.default.gold_kpi_volume_pedidos_5min       ← Gold (overwrite)
workspace.default.gold_kpi_ticket_medio_por_pagamento  ← Gold (overwrite)
workspace.default.gold_kpi_taxa_cancelamento_hora     ← Gold (overwrite)
workspace.default.gold_hist_*                           ← Histórico (append)
```

### Compute
- **Tipo:** Serverless (AWS, CPU)
- **Restrição:** JDBC genérico para SQL Server não funciona no serverless (`UNSUPPORTED_DATA_SOURCE_WRITE`)
- **Leitura:** Via Lakehouse Federation (Unity Catalog foreign catalog `sqlserver_catalog.squad2.*`)
- **Escrita:** Delta no schema `workspace.default`

---

## 2. Notebook Silver: 05_ingestao_silver_le

**ID:** 2960303772837510  
**Path:** `/Repos/engenheiro_leandroreis@hotmail.com/estagio-empregadados-turma-2/05_ingestao_silver_le`  
**Linguagem:** Python  

### 2.1 Objetivo

Processar a tabela `ecommerce_clientes` da origem SQL Server, aplicando 10 regras de qualidade, persistindo registros válidos na Silver (MERGE) e rejeitados na Quarentena (APPEND). Calcula 3 KPIs de negócio.

### 2.2 Origem e Destino

| Camada | Tabela | UC Location |
| --- | --- | --- |
| Origem | ecommerce_clientes | `sqlserver_catalog.squad2.ecommerce_clientes` |
| Silver | silver_ecommerce_clientes | `workspace.default.silver_ecommerce_clientes` |
| Quarentena | quarentena_ecommerce_clientes | `workspace.default.quarentena_ecommerce_clientes` |
| Referência (KPI 09) | ecommerce_pedidos | `sqlserver_catalog.squad2.ecommerce_pedidos` |

### 2.3 Estrutura do Notebook (13 células)

| Célula | Tipo | Conteúdo |
| --- | --- | --- |
| 1 | Markdown | Título + 10 regras |
| 2 | `%run` | Setup config (`./00_setup_config`) |
| 3 | Python | Config: imports, tabelas, funções auxiliares |
| 4-5 | MD + Python | Leitura e filtro incremental (anti-join) |
| 6-7 | MD + Python | Regras 01-05, 08, 10 (validação + alertas) |
| 8-9 | MD + Python | Persistência (merge Silver + append quarentena) |
| 10-11 | MD + Python | KPIs 06, 07, 09 |
| 12-13 | MD + Python | Resumo geral |

### 2.4 Regras de Qualidade

| # | Regra | Validação | Rejeição |
| --- | --- | --- | --- |
| 01 | Schema completo em cada micro-lote | Colunas: `id_cliente`, `uuid_cliente`, `nome`, `sobrenome`, `email`, `dt_cadastro` | Bloqueia lote (raise ValueError) |
| 02 | Email único no lote e não existir na Silver | Window por email + anti-join com Silver | `EMAIL_DUPLICADO_NO_LOTE` / `EMAIL_EXISTE_NA_SILVER` |
| 03 | dt_cadastro parseável e não futura | `try_to_timestamp()` + comparação com `current_timestamp()` | `DT_CADASTRO_INVALIDA` / `DT_CADASTRO_FUTURA` |
| 04 | uuid_cliente formato UUID v4 válido | Regex: `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$` | `UUID_INVALIDO` |
| 05 | Deduplicação de id_cliente e email | `row_number()` por `id_cliente` (mantém mais recente) | Descarta duplicados |
| 06 | KPI: Novos clientes nos últimos 10 minutos | Filtro `silver_processed_at >= now() - 10 min` | — |
| 07 | KPI: Cadastros por provedor de email | `regexp_extract(email, '@([^.]+)', 1)` + `groupBy` | Alerta se >30% provedores desconhecidos |
| 08 | Alertar se email em mais de 1 id_cliente | `collect_set(id_cliente)` por email | `EMAIL_DUPLICADO_NO_LOTE` + alerta no log |
| 09 | KPI: Tempo médio até primeira compra | Join com `ecommerce_pedidos` por `id_cliente`, `datediff` | — |
| 10 | Alertar se dt_ultima_atualizacao < dt_cadastro | Comparação de timestamps | `DT_ATUALIZACAO_ANTERIOR_AO_CADASTRO` + alerta no log |

### 2.5 Funções Auxiliares

```python
tabela_existe(nome_tabela)  # Verifica se tabela UC existe
merge_into(df, tabela_uc, chave)  # MERGE INTO por chave (upsert idempotente)
append_quarentena(df, tabela_quar)  # APPEND em tabela de quarentena
```

### 2.6 Persistência

| Destino | Modo | Função |
| --- | --- | --- |
| Silver | MERGE (upsert por `id_cliente`) | `merge_into()` |
| Quarentena | APPEND (mantém histórico) | `append_quarentena()` |

### 2.7 KPIs

| KPI | Descrição | Cálculo |
| --- | --- | --- |
| 06 | Novos clientes (10 min) | `COUNT(*) WHERE silver_processed_at >= now() - 10 min` |
| 07 | Cadastros por provedor | `regexp_extract(email, '@([^.]+)') GROUP BY provedor` |
| 09 | Tempo até 1ª compra | `AVG(datediff(dt_primeira_compra, dt_cadastro))` via join com pedidos |

### 2.8 Resultados da Última Execução

| Métrica | Valor |
| --- | --- |
| Lote (novos) | 10 registros |
| DT_CADASTRO_FUTURA | 8 rejeitados |
| EMAIL_DUPLICADO_NO_LOTE | 2 rejeitados |
| Válidos para Silver | 0 |
| Total Silver | 363 registros |
| Total Quarentena | 30 registros |
| KPI 06 (novos 10min) | 0 |
| KPI 07 (provedores) | gmail 45%, hotmail 23%, outlook 19%, yahoo 5% |
| KPI 09 (tempo até compra) | 701 dias (4 clientes com compra) |
| Alerta 08 | 🚨 Email duplicado no lote |

---

## 3. Notebook Gold: 06_ingestao_gold_le

**ID:** 2960303772837509  
**Path:** `/Repos/engenheiro_leandroreis@hotmail.com/estagio-empregadados-turma-2/06_ingestao_gold_le`  
**Linguagem:** SQL (análises) + Python (config e JDBC)  

### 3.1 Objetivo

Ler a Silver de pedidos e calcular 3 KPIs de negócio com janelas de tempo. Persistir em tabelas Gold (overwrite) e histórico (append) em Delta. Inserir no SQL Server via JDBC (quando disponível).

### 3.2 Origem e Destino

| Camada | Tabela | UC Location |
| --- | --- | --- |
| Origem | silver_ecommerce_pedidos | `workspace.default.silver_ecommerce_pedidos` |
| Gold (overwrite) | gold_kpi_volume_pedidos_5min | `workspace.default.gold_kpi_volume_pedidos_5min` |
| Gold (overwrite) | gold_kpi_ticket_medio_por_pagamento | `workspace.default.gold_kpi_ticket_medio_por_pagamento` |
| Gold (overwrite) | gold_kpi_taxa_cancelamento_hora | `workspace.default.gold_kpi_taxa_cancelamento_hora` |
| Histórico (append) | gold_hist_volume_pedidos_5min | `workspace.default.gold_hist_volume_pedidos_5min` |
| Histórico (append) | gold_hist_ticket_medio_por_pagamento | `workspace.default.gold_hist_ticket_medio_por_pagamento` |
| Histórico (append) | gold_hist_taxa_cancelamento_hora | `workspace.default.gold_hist_taxa_cancelamento_hora` |

### 3.3 Estrutura do Notebook (15 células)

| Célula | Tipo | Conteúdo |
| --- | --- | --- |
| 1 | Markdown | Título + KPIs |
| 2 | `%run` | Setup config |
| 3 | Python | Config (tabelas, autoOptimize) |
| 4-5 | MD + SQL | KPI 1 — Volume de pedidos (5 min) |
| 6-7 | MD + SQL | KPI 2 — Ticket médio por pagamento |
| 8-9 | MD + SQL | KPI 3 — Taxa de cancelamento (hora) |
| 10-11 | MD + SQL | Persistência Delta (overwrite + append) |
| 12-13 | MD + Python | Persistência SQL Server (JDBC) |
| 14-15 | MD + SQL | Resumo geral |

### 3.4 KPIs Calculados

| KPI | Temp View | Tabela Gold | Query |
| --- | --- | --- | --- |
| 1 — Volume pedidos (5 min) | `vw_kpi1` | `gold_kpi_volume_pedidos_5min` | `window(dt_pedido, '5 minutes')` → COUNT, SUM, AVG |
| 2 — Ticket médio por pagamento | `vw_kpi2` | `gold_kpi_ticket_medio_por_pagamento` | `GROUP BY UPPER(TRIM(metodo_pagamento))` → COUNT, SUM, AVG |
| 3 — Taxa cancelamento (hora) | `vw_kpi3` | `gold_kpi_taxa_cancelamento_hora` | `window(dt_pedido, '1 hour')` → % cancelados |

### 3.5 Persistência

| Destino | Modo | SQL |
| --- | --- | --- |
| Gold | OVERWRITE | `CREATE OR REPLACE TABLE ... USING DELTA AS SELECT * FROM vw_kpiN` |
| Histórico | APPEND | `CREATE TABLE IF NOT EXISTS ... WHERE 1=0` + `INSERT INTO ... SELECT * FROM vw_kpiN` |
| SQL Server | OVERWRITE (Gold) + APPEND (Hist) | `df.write.jdbc()` — **não funciona no serverless** |

> **✅ Solução aplicada:** O serverless suporta escrita no SQL Server usando o connector nativo `format("sqlserver")` (bundled connector), em vez do JDBC genérico (`df.write.jdbc()`). A célula foi corrigida e testada com sucesso. Ver detalhes na seção 3.7.

### 3.6 Resultados da Última Execução

| KPI | Temp View | Gold | Histórico |
| --- | --- | --- | --- |
| Volume pedidos (5 min) | 65.504 | 65.504 | 458.528 |
| Ticket médio por pagamento | 4 | 4 | 28 |
| Taxa cancelamento (hora) | 17.528 | 17.528 | 122.696 |

**KPI 2 — Detalhamento por método de pagamento:**

| Método | Pedidos | Valor Total | Ticket Médio |
| --- | --- | --- | --- |
| CARTÃO DE CRÉDITO | 58.662 | R$ 49.410.613 | R$ 842 |
| PIX | 27.180 | R$ 22.924.473 | R$ 843 |
| BOLETO | 4.499 | R$ 3.733.557 | R$ 830 |
| CARTÃO DÉBITO | 2 | R$ 340 | R$ 170 |

### 3.7 Persistência SQL Server — Solução Serverless

#### Problema

O serverless compute bloqueia o driver JDBC genérico (`JdbcRelationProvider`) usado por `df.write.jdbc()`:

```
UNSUPPORTED_DATA_SOURCE_WRITE: The input query contains unsupported data source(s).
Only csv, json, avro, delta, kafka, parquet, orc, text, unity_catalog, binaryFile,
xml, excel, simplescan, iceberg, file, mysql, postgresql, sqlserver, snowflake,
redshift data sources are allowed to run DML on serverless compute.
```

Apesar de `sqlserver` aparecer na lista de fontes suportadas, o metodo `df.write.jdbc()` mapeia para o `JdbcRelationProvider` generico, que **nao é** o mesmo que o connector nativo `sqlserver`.

#### Solucao

A documentacao oficial do Databricks confirma que **SQL Server é suportado para escrita no serverless** usando o connector nativo bundled `format("sqlserver")`:

> *"Writes on serverless compute are supported for PostgreSQL, SQL Server, MySQL, Snowflake, and Redshift."*  
> Fonte: [Spark data sources — Databricks documentation](https://docs.databricks.com/aws/en/connect/spark-data-sources/)

#### Codigo — Antes (nao funciona) vs Depois (funciona)

```python
# ANTES — JDBC generico (bloqueado no serverless)
df.write.mode("overwrite").jdbc(jdbc_url, tabela, properties=jdbc_props)

# DEPOIS — Connector nativo sqlserver (suportado no serverless)
df.write.format("sqlserver")
    .option("host", SQL_HOST)
    .option("port", "1433")
    .option("database", SQL_DATABASE)
    .option("user", SQL_USERNAME)
    .option("password", SQL_PASSWORD)
    .option("dbtable", tabela_sql)
    .option("encrypt", "true")
    .option("trustServerCertificate", "true")
    .mode("overwrite")
    .save()
```

#### Opcoes suportadas no serverless

| Opcao | Descricao |
| --- | --- |
| `host` | Hostname do SQL Server |
| `port` | Porta (default: 1433) |
| `database` | Nome do banco |
| `user` | Usuario |
| `password` | Senha |
| `dbtable` | Tabela destino (suporta schema.table) |
| `encrypt` | Criptografia TLS (default: false) |
| `trustServerCertificate` | Confiar no certificado TLS sem validacao |
| `batchsize` | Linhas por batch (default: 1000) |
| `numPartitions` | Particoes para escrita paralela |
| `truncate` | Truncar em modo overwrite (em vez de drop/create) |
| `isolationLevel` | Nivel de isolamento (default: READ_UNCOMMITTED) |

#### Resultado do teste

```
=== GOLD (OVERWRITE) no SQL Server ===
  gold_kpi_volume_pedidos_5min: 65504 registros
  gold_kpi_ticket_medio_por_pagamento: 4 registros
  gold_kpi_taxa_cancelamento_hora: 17528 registros

=== HISTORICO (APPEND) no SQL Server ===
  gold_hist_volume_pedidos_5min: 65504 registros
  gold_hist_ticket_medio_por_pagamento: 4 registros
  gold_hist_taxa_cancelamento_hora: 17528 registros
```

---

## 4. Resumo das Tabelas

| Tabela | Camada | Persistência | Registros |
| --- | --- | --- | --- |
| silver_ecommerce_clientes | Silver | MERGE (id_cliente) | 363 |
| quarentena_ecommerce_clientes | Quarentena | APPEND | 30 |
| gold_kpi_volume_pedidos_5min | Gold | OVERWRITE | 65.504 |
| gold_kpi_ticket_medio_por_pagamento | Gold | OVERWRITE | 4 |
| gold_kpi_taxa_cancelamento_hora | Gold | OVERWRITE | 17.528 |
| gold_hist_volume_pedidos_5min | Histórico | APPEND | 458.528 |
| gold_hist_ticket_medio_por_pagamento | Histórico | APPEND | 28 |
| gold_hist_taxa_cancelamento_hora | Histórico | APPEND | 122.696 |

---

## 5. Configuração de Ambiente

### 5.1 Setup (`00_setup_config`)

O notebook `%run ./00_setup_config` valida as credenciais do `.env`:

| Credencial | Status |
| --- | --- |
| ADLS_CLIENT_ID | OK |
| ADLS_TENANT_ID | OK |
| ADLS_CLIENT_SECRET | OK |
| SQL_HOST | OK |
| SQL_DATABASE | OK |
| SQL_USERNAME | OK |
| SQL_PASSWORD | OK |

### 5.2 Otimização Delta

```python
spark.conf.set("spark.databricks.delta.properties.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.autoOptimize.autoCompact", "true")
```

> No serverless, essas configs não estão disponíveis — o notebook usa try/except e prossegue com o padrão.

---

## 6. Fluxo de Execução

### Silver (05_ingestao_silver_le)

1. `%run ./00_setup_config` — valida credenciais
2. Config — define tabelas, parâmetros e funções
3. Leitura — lê origem via Lakehouse Federation, anti-join por `id_cliente`
4. Regras — aplica 10 validações, separa válidos e rejeitados
5. Persistência — `merge_into` Silver + `append_quarentena` quarentena
6. KPIs — calcula 06, 07, 09 a partir da Silver
7. Resumo — consolida resultados

### Gold (06_ingestao_gold_le)

1. `%run ./00_setup_config` — valida credenciais
2. Config — define tabelas Gold e histórico
3. KPI 1 — `CREATE TEMP VIEW vw_kpi1` (volume 5 min)
4. KPI 2 — `CREATE TEMP VIEW vw_kpi2` (ticket médio)
5. KPI 3 — `CREATE TEMP VIEW vw_kpi3` (cancelamento hora)
6. Persistência Delta — `CREATE OR REPLACE TABLE` (Gold) + `INSERT INTO` (histórico)
7. Persistência SQL Server — `df.write.jdbc()` (quando em cluster clássico)
8. Resumo — `SELECT UNION ALL` consolidando temp views, Gold e histórico

---

## 7. Limitações e Considerações

| Item | Detalhe |
| --- | --- |
| JDBC no serverless | **Resolvido.** Usar `format("sqlserver")` (connector nativo bundled) em vez de `df.write.jdbc()` (JDBC generico). Ver secao 3.7. |
| ADLS no serverless | Bloqueado (outbound network). Dados lidos via Lakehouse Federation. |
| autoOptimize | Não disponível no serverless — tratado com try/except. |
| KPI 09 (tempo até compra) | Depende de join com `ecommerce_pedidos`. Retorna 0 se não houver match. |
| Histórico crescente | Tabelas de histórico crescem a cada execução (append). Considerar purga periódica. |

---

**Fim da documentação.**