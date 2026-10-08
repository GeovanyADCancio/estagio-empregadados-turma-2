# Squad 3 — Batch | food_estoque_lojas + food_avaliacoes_produto

Notebooks desenvolvidos durante o programa de estágio **estagio-empregadados-turma-2**, referentes às tabelas sob responsabilidade do Squad 3 (Batch).

Este README documenta o fluxo de dados das tabelas `food_estoque_lojas` e `food_avaliacoes_produto`, desde a ingestão dos dados brutos até a disponibilização das regras de negócio na camada Gold e no SQL Server.

---

## Tabelas sob responsabilidade

| **Tabela** | **Fonte (Raw)** | **Bronze** | **Silver** | **Gold (Delta)** | **Gold (SQL Server)** |
|---|---|---|---|---|---|
| `food_estoque_lojas` | `raw/batch-data/` | `squad3/bronze/food_estoque_lojas` | `squad3/silver/food_estoque_lojas` | `gold/food_estoque_lojas/*` | `squad3.gold_*` |
| `food_avaliacoes_produto` | `raw/batch-data/` | `squad3/bronze/food_avaliacoes_produto` | `squad3/silver/food_avaliacoes_produto` | `gold/food_avaliacoes_produto/*` | `squad3.gold_*` |

---

## Arquitetura Medalhão

```text
Raw (CSV)
    ↓
Bronze (Delta)
    ↓
Silver (Delta + flags)
    ↓
Gold (Delta + regras de negócio)
    ↓
SQL Server
    ↓
Power BI / consumo analítico
```

### Estratégia por camada

```text
Raw      → origem dos arquivos CSV
Bronze   → append incremental + auditoria
Silver   → tratamento + validações + flags + upsert
Gold     → regras de negócio + KPIs + alertas + enriquecimentos
SQL      → tabelas finais para consumo analítico
```

---

# 1. Camada Bronze

A Bronze recebe os arquivos CSV do container `raw` e os armazena em Delta no container `squad3`.

### Características

- **Modo:** `append` incremental
- **Formato:** Delta Lake
- **Auditoria:** `bronze_ingested_at`
- **Origem do arquivo:** `bronze_source_file`
- **Particionamento:** `ano`, `mes`
- **Frequência:** semanal
- **Idempotência:** o mesmo arquivo não deve ser ingerido novamente sem alteração

A Bronze representa os dados próximos da origem, preservando o histórico necessário para as camadas seguintes.

---

# 2. Camada Silver

A Silver lê os dados da Bronze e aplica os tratamentos técnicos e validações necessários.

As duas tabelas possuem regras próprias, mas seguem o mesmo princípio:

> **Dados tecnicamente inválidos não são simplesmente descartados quando podem ser preservados; eles recebem flags para que a camada Gold decida como utilizá-los.**

### Características

- **Formato:** Delta Lake
- **Modo:** upsert/merge lógico por DataFrame
- **Auditoria:** `silver_processed_at`
- **Particionamento:** `ano`, `mes`
- **Deduplicação:** baseada na chave da tabela
- **Tratamento:** casts explícitos, trim e normalização de strings
- **Validações:** regras técnicas e flags de qualidade

No ambiente Databricks Serverless/Spark Connect, o upsert da Silver é implementado sem depender de `DeltaTable.merge()`.

A estratégia utilizada é:

```text
Dados novos
    ↓
Identifica chaves afetadas
    ↓
LEFT ANTI nos dados antigos
    ↓
UNION BY NAME
    ↓
OVERWRITE da tabela Delta
```

---

# 3. food_estoque_lojas

A tabela `food_estoque_lojas` representa snapshots de estoque das lojas.

A chave lógica utilizada para identificar um registro é:

```text
id_loja + sku + id_lote + dt_snapshot
```

A camada Gold utiliza os snapshots da Silver para gerar KPIs e alertas operacionais.

---

## 3.1 Regras Gold — food_estoque_lojas

São produzidas 5 tabelas Gold.

| # | Tabela Gold | Grão / Chave | Estratégia | Regra |
|---|---|---|---|---|
| 6 | `gold_kpi_ruptura_estoque` | `id_loja + mes_snapshot` | MERGE | KPI de ruptura |
| 7 | `gold_kpi_lotes_vencidos` | `id_loja + mes_snapshot` | MERGE | KPI de lotes vencidos |
| 8 | `gold_kpi_giro_medio_estoque` | `tipo_produto + categoria_pai` | MERGE | Giro médio |
| 9 | `gold_alerta_estoque_negativo` | `id_loja + sku + id_lote + dt_snapshot` | MERGE | Estoque negativo |
| 10 | `gold_regra10_ruptura_queda_vendas` | `id_loja + sku + dt_snapshot` | MERGE | Ruptura + queda de vendas |

---

## 3.2 Regra 6 — KPI de ruptura de estoque

Tabela:

```text
squad3.gold_kpi_ruptura_estoque
```

Grão:

```text
id_loja + mes_snapshot
```

Principais campos:

- `id_loja`
- `nome_loja`
- `mes_snapshot`
- `qtd_snapshots`
- `qtd_snapshots_ruptura`
- `taxa_ruptura_pct`
- `gold_ingested_at`

A regra consolida os snapshots de estoque por loja e mês e calcula a taxa de ruptura.

### Estratégia SQL

```text
STAGING
   ↓
MERGE
   ↓
gold_kpi_ruptura_estoque
   ↓
DROP STAGING
```

---

## 3.3 Regra 7 — KPI de lotes vencidos

Tabela:

```text
squad3.gold_kpi_lotes_vencidos
```

Grão:

```text
id_loja + mes_snapshot
```

Principais campos:

- `id_loja`
- `nome_loja`
- `mes_snapshot`
- `qtd_snapshots`
- `qtd_snapshots_lote_vencido`
- `pct_snapshots_lote_vencido`
- `gold_ingested_at`

O objetivo é identificar a ocorrência de snapshots relacionados a lotes vencidos por loja e mês.

---

## 3.4 Regra 8 — Giro médio de estoque

Tabela:

```text
squad3.gold_kpi_giro_medio_estoque
```

Grão:

```text
tipo_produto + categoria_pai
```

Principais campos:

- `tipo_produto`
- `categoria_pai`
- `giro_medio_dias`
- `gold_ingested_at`

Essa tabela consolida o giro médio do estoque por tipo de produto e categoria pai.

Antes de aplicar a PK no SQL Server, as colunas `tipo_produto` e `categoria_pai` devem ser verificadas para garantir que não existam valores nulos.

---

## 3.5 Regra 9 — Alerta de estoque negativo

Tabela:

```text
squad3.gold_alerta_estoque_negativo
```

Grão:

```text
id_loja + sku + id_lote + dt_snapshot
```

Principais campos:

- `id_loja`
- `sku`
- `id_lote`
- `quantidade_disponivel`
- `dt_snapshot`
- `flag_quantidade_invalida`
- `gold_ingested_at`

A regra identifica situações de quantidade de estoque inválida/negativa e disponibiliza o registro para análise operacional.

As colunas da chave precisam ser validadas contra nulos antes da criação da PK no SQL Server.

---

## 3.6 Regra 10 — Ruptura coincidente com queda de vendas

Tabela:

```text
squad3.gold_regra10_ruptura_queda_vendas
```

Grão:

```text
id_loja + sku + dt_snapshot
```

Principais campos:

- `id_loja`
- `sku`
- `dt_snapshot`
- `quantidade_disponivel`
- `estoque_minimo`
- `quantidade_vendida`
- `quantidade_vendida_anterior`
- `flag_ruptura_coincide_queda_vendas`
- `gold_ingested_at`

A regra cruza a situação do estoque com a quantidade vendida no período atual e no período anterior para identificar casos em que uma ruptura coincide com queda nas vendas.

---

# 4. food_avaliacoes_produto

A tabela `food_avaliacoes_produto` representa as avaliações realizadas sobre produtos/pedidos.

A chave principal da avaliação é:

```text
id_avaliacao
```

A camada Gold utiliza as avaliações para validações temporais, KPIs, rankings e enriquecimento com informações de pedido, lote e fornecedor.

---

## 4.1 Regras Gold — food_avaliacoes_produto

São produzidas 5 tabelas Gold.

| # | Tabela Gold | Grão / Chave | Estratégia | Regra |
|---|---|---|---|---|
| 6 | `gold_avaliacoes_antes_pedido` | `id_avaliacao` | MERGE | Avaliação antes do pedido |
| 7 | `gold_kpi_nota_sku_mes` | `sku + mes_avaliacao` | MERGE | Nota média por SKU/mês |
| 8 | `gold_kpi_avaliacoes_mes` | `mes_avaliacao` | MERGE | Volume/status das avaliações |
| 9 | `gold_kpi_top10_piores_skus` | `sku + trimestre_avaliacao` | DELETE + INSERT | Top 10 piores SKUs |
| 10 | `gold_avaliacoes_enriquecidas` | `id_avaliacao + id_lote` | MERGE | Avaliação enriquecida |

---

## 4.2 Regra 6 — Avaliação antes do pedido

Tabela:

```text
squad3.gold_avaliacoes_antes_pedido
```

Grão:

```text
id_avaliacao
```

Principais campos:

- `id_avaliacao`
- `id_pedido`
- `verificada`
- `dt_avaliacao`
- `flag_avaliacao_antes_pedido`
- `gold_ingested_at`

A regra verifica se uma avaliação verificada ocorreu antes da data do pedido associado.

A validação só é aplicada quando:

- a avaliação é verificada;
- o pedido é válido;
- existe referência válida ao pedido.

---

## 4.3 Regra 7 — Nota média por SKU e mês

Tabela:

```text
squad3.gold_kpi_nota_sku_mes
```

Grão:

```text
sku + mes_avaliacao
```

Principais campos:

- `sku`
- `mes_avaliacao`
- `nota_media`
- `gold_ingested_at`

A avaliação é agrupada por SKU e mês para gerar a nota média.

---

## 4.4 Regra 8 — KPI de avaliações por mês

Tabela:

```text
squad3.gold_kpi_avaliacoes_mes
```

Grão:

```text
mes_avaliacao
```

Principais campos:

- `mes_avaliacao`
- `qtd_avaliacoes`
- `qtd_avaliacoes_verificadas`
- `qtd_avaliacoes_nao_verificadas`
- `pct_avaliacoes_verificadas`
- `pct_avaliacoes_nao_verificadas`
- `gold_ingested_at`

Essa tabela permite acompanhar o volume mensal de avaliações e a proporção entre avaliações verificadas e não verificadas.

---

## 4.5 Regra 9 — Top 10 piores SKUs

Tabela:

```text
squad3.gold_kpi_top10_piores_skus
```

Grão:

```text
sku + trimestre_avaliacao
```

Principais campos:

- `sku`
- `trimestre_avaliacao`
- `qtd_avaliacoes`
- `nota_media`
- `ranking`
- `gold_ingested_at`

A regra considera somente SKUs com pelo menos 5 avaliações no trimestre.

O ranking é feito pela menor nota média.

### Por que não usar apenas MERGE?

Porque um SKU pode estar no Top 10 em uma execução e deixar de estar no Top 10 na execução seguinte.

Por isso, a atualização é feita assim:

```text
Identifica os trimestres afetados
        ↓
DELETE dos registros desses trimestres
        ↓
INSERT do novo Top 10
```

Dessa forma, SKUs que deixaram de pertencer ao Top 10 não permanecem indevidamente na tabela.

---

## 4.6 Regra 10 — Avaliações enriquecidas

Tabela:

```text
squad3.gold_avaliacoes_enriquecidas
```

Grão:

```text
id_avaliacao + id_lote
```

Principais campos:

- `id_avaliacao`
- `id_pedido`
- `sku`
- `nota`
- `dt_avaliacao`
- `verificada`
- `id_lote`
- `id_fornecedor`
- `fornecedor`
- `gold_ingested_at`

O enriquecimento cruza a avaliação com:

```text
food_avaliacoes_produto
        ↓
ecommerce_itens_pedido
        ↓
food_lotes_producao
        ↓
food_fornecedores
```

Como não existe `id_lote` diretamente no item do pedido, o cruzamento com lotes é realizado por SKU. Consequentemente, uma avaliação pode ser associada a mais de um lote.

Por isso, o grão final é:

```text
id_avaliacao + id_lote
```

Antes de criar a PK no SQL Server, é necessário garantir que `id_avaliacao` e `id_lote` não sejam nulos.

---

# 5. Auditoria Gold

Todas as tabelas Gold possuem:

```text
gold_ingested_at
```

Esse campo registra o momento em que o processamento Gold foi executado.

O timestamp é definido uma única vez no início do processamento:

```python
from datetime import datetime

gold_ingested_at = datetime.now()
```

E aplicado às tabelas finais:

```python
.withColumn(
    "gold_ingested_at",
    lit(gold_ingested_at)
)
```

No SQL Server:

```sql
gold_ingested_at DATETIME2(3) NOT NULL
```

---

# 6. Persistência Gold no SQL Server

O ambiente utiliza Databricks Serverless / Spark Connect.

Por isso, existem dois padrões diferentes.

### Escrita de dados

Para gravar DataFrames em tabelas de staging:

```python
df.write \
    .format("sqlserver") \
    .option("host", jdbc_host) \
    .option("port", "1433") \
    .option("database", jdbc_database) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_username) \
    .option("password", jdbc_password) \
    .option("encrypt", "true") \
    .option("trustServerCertificate", "false") \
    .mode("overwrite") \
    .save()
```

### Execução de SQL

Para `CREATE TABLE`, `MERGE`, `DELETE`, `INSERT` e `DROP TABLE`:

```python
(
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", "SELECT 1")
    .option("sessionInitStatement", sql_comando)
    .options(**jdbc_properties)
    .load()
)
```

Esse padrão é utilizado devido às restrições do Databricks Serverless/Spark Connect.

---

# 7. Padrão de staging

As tabelas Gold SQL utilizam uma tabela de staging durante a carga.

Exemplo:

```text
DataFrame
   ↓
squad3.stg_gold_kpi_ruptura_estoque
   ↓
MERGE
   ↓
squad3.gold_kpi_ruptura_estoque
   ↓
DROP staging
```

A tabela `stg_gold_*` é intermediária e não representa a tabela Gold definitiva.

Ao final de uma execução bem-sucedida, deve permanecer somente a tabela:

```text
squad3.gold_*
```

---

# 8. Estratégias de atualização

## Tabelas acumulativas

Utilizam `MERGE` por chave:

```text
Nova execução
     ↓
Staging
     ↓
MERGE
     ├── chave existente → UPDATE
     └── chave nova      → INSERT
```

Aplicado às regras:

- Estoque 6
- Estoque 7
- Estoque 8
- Estoque 9
- Estoque 10
- Avaliações 6
- Avaliações 7
- Avaliações 8
- Avaliações 10

## Top 10

A Regra 9 de avaliações utiliza:

```text
DELETE dos períodos afetados
        ↓
INSERT do ranking recalculado
```

Isso garante que registros que saíram do ranking também sejam removidos.

---

# 9. Estrutura de caminhos Delta

## food_estoque_lojas

```text
gold/
└── food_estoque_lojas/
    ├── gold_kpi_ruptura_estoque/
    ├── gold_kpi_lotes_vencidos/
    ├── gold_kpi_giro_medio_estoque/
    ├── gold_alerta_estoque_negativo/
    └── gold_regra10_ruptura_queda_vendas/
```

## food_avaliacoes_produto

```text
gold/
└── food_avaliacoes_produto/
    ├── gold_avaliacoes_antes_pedido/
    ├── gold_kpi_nota_sku_mes/
    ├── gold_kpi_avaliacoes_mes/
    ├── gold_kpi_top10_piores_skus/
    └── gold_avaliacoes_enriquecidas/
```

Bronze e Silver permanecem em caminhos independentes:

```text
squad3/bronze/...
squad3/silver/...
```

A Bronze não fica dentro da Raw.

---

# 10. Fluxo de execução

```text
                    ┌──────────────────────┐
                    │      Raw / CSV       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │       Bronze         │
                    │      append          │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │       Silver         │
                    │ tratamento + flags   │
                    │       upsert         │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
     ┌────────────────────┐       ┌────────────────────┐
     │ food_estoque_lojas │       │food_avaliacoes_prod│
     └─────────┬──────────┘       └─────────┬──────────┘
               │                            │
               ▼                            ▼
        ┌──────────────┐             ┌──────────────┐
        │     Gold     │             │     Gold     │
        │ 5 tabelas    │             │ 5 tabelas    │
        └──────┬───────┘             └──────┬───────┘
               │                            │
               └──────────────┬─────────────┘
                              ▼
                    ┌──────────────────────┐
                    │     SQL Server       │
                    │      schema squad3   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Power BI        │
                    └──────────────────────┘
```

---

# 11. Stack

- **Plataforma:** Databricks Serverless
- **Runtime:** Spark / Spark Connect
- **Data Lake:** Azure Data Lake Storage Gen2
- **Formato:** Delta Lake
- **Banco analítico:** Azure SQL Server
- **Schema SQL:** `squad3`
- **Conector de escrita:** `format("sqlserver")`
- **Execução de DDL/DML:** JDBC + `sessionInitStatement`
- **Visualização:** Power BI

---

# 12. Restrições importantes do ambiente

- `spark._jvm.java.sql.DriverManager` não é utilizado no Serverless/Spark Connect.
- A escrita JDBC tradicional (`format("jdbc")`) não é utilizada para gravar as stagings no Serverless; utiliza-se o datasource nativo `sqlserver`.
- Comandos SQL como `CREATE`, `MERGE`, `DELETE`, `INSERT` e `DROP` são executados via Spark JDBC Reader usando `sessionInitStatement`.
- A estratégia de upsert Delta da Silver não depende de `DeltaTable.merge()`.
- O acesso ao ADLS utiliza autenticação via Service Principal.
- Credenciais não devem ser hardcoded nos notebooks.

---

# 13. Segurança

- Credenciais SQL/ADLS devem permanecer fora do código.
- Arquivos de credenciais devem estar fora do repositório Git.
- O container `raw` é somente leitura para o fluxo da Squad.
- A Squad escreve nas áreas `bronze`, `silver` e `gold` do container `squad3`.
- Tabelas de outras squads são somente lidas quando necessárias para enriquecimento ou validação.
- As tabelas SQL são criadas de forma idempotente usando `IF OBJECT_ID(...) IS NULL`.

---

# 14. Resumo das tabelas Gold

### food_estoque_lojas

```text
1. gold_kpi_ruptura_estoque
2. gold_kpi_lotes_vencidos
3. gold_kpi_giro_medio_estoque
4. gold_alerta_estoque_negativo
5. gold_regra10_ruptura_queda_vendas
```

### food_avaliacoes_produto

```text
6. gold_avaliacoes_antes_pedido
7. gold_kpi_nota_sku_mes
8. gold_kpi_avaliacoes_mes
9. gold_kpi_top10_piores_skus
10. gold_avaliacoes_enriquecidas
```

Total:

```text
10 tabelas Gold
5 de food_estoque_lojas
5 de food_avaliacoes_produto
```

---

## Observação

A estrutura deste README segue a organização e o nível de documentação do README de referência fornecido, adaptando as tabelas, regras Gold, estratégias de persistência e particularidades do ambiente das tabelas `food_estoque_lojas` e `food_avaliacoes_produto`.
