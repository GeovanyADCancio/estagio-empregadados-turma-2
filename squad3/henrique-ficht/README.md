# Squad 3 — Batch | Henrique Ficht

Notebooks desenvolvidos durante o programa de estágio **estagio-empregadados-turma-2**,
referentes às tabelas sob responsabilidade do Squad 3 (Batch).

---

## Tabelas sob responsabilidade

| Tabela | Fonte (Raw) | Bronze | Silver | Gold (Delta) | Gold (SQL) |
|---|---|---|---|---|---|
| `food_fornecedores` | `raw/batch-data/` | `squad3/bronze/food_fornecedores` | `squad3/silver/food_fornecedores` | 5 tabelas sumarizadas | `squad3.gold_fornecedor_*` |
| `food_lotes_producao` | `raw/batch-data/` | `squad3/bronze/food_lotes_producao` | `squad3/silver/food_lotes_producao` | 5 tabelas sumarizadas | `squad3.gold_lote_*` |

---

## Arquitetura Medalhão

```
Raw (CSV)  →  Bronze (Delta)  →  Silver (Delta + flags)  →  Gold (Delta + SQL Server)
              append              merge (upsert por PK)      merge / overwrite
              semanal             semanal                    semanal
```

### Camada Bronze — Ingestão crua com auditoria

Lê os CSVs históricos do container `raw` e salva em Delta no container `squad3/bronze/`,
adicionando colunas de auditoria e particionamento.

- **Modo:** `append` incremental — verifica `modificationTime` do CSV e só appenda se o arquivo foi modificado desde a última ingestão (compara contra `MAX(bronze_ingested_at)` da Bronze)
- **Auditoria:** `bronze_ingested_at` (timestamp da ingestão), `bronze_source_file` (path do CSV via `_metadata.file_path`)
- **Partição:** `ano`, `mes` (extraídos de `dt_cadastro` / `dt_fabricacao`)
- **Frequência:** semanal (toda segunda-feira)
- **Idempotência:** rodar o notebook múltiplas vezes com o mesmo CSV não gera duplicatas na Bronze

### Camada Silver — Limpeza, tratamento e validação

Lê os Deltas da Bronze, aplica regras técnicas da planilha de regras (tabelas 'food_fornecedores' e 'food_lotes_producao'),
e salva em Delta no container `squad3/silver/`.

- **Modo:** `merge` manual (upsert por PK — implementado via DataFrame por restrição do Unity Catalog)
- **Auditoria:** `silver_processed_at`
- **Partição:** `ano`, `mes`

**Tratamentos aplicados:**

- Deduplicação por PK (mantém registro mais recente por `bronze_ingested_at`)
- Cast explícito de tipos (INT, BOOLEAN, TIMESTAMP, STRING)
- Trim em todas as colunas string
- CNPJ: remoção de pontuação + `lpad` seguro (somente 13→14 dígitos)
- Certificações: trim individual de cada item separado por `;`
- UF: `upper()` + `trim()`
- Remoção das colunas de auditoria da Bronze

**Regras técnicas — food_fornecedores:**

| Regra | Validação | Ação |
|---|---|---|
| `id_fornecedor` PK | Não nulo, não duplicado | Remove |
| `cnpj` 14 dígitos | Flag antes do tratamento, lpad só para 13 dígitos | Flag `_flag_cnpj_invalido` |
| `categoria_fornecida` | Validada contra Silvers de referência (dinâmico) | Flag `_flag_categoria_invalida` |
| `tipo_fornecedor` | Lista permitida (7 tipos) | Flag `_flag_tipo_invalido` |
| `lead_time_dias` > 0 | Range por tipo (1-3 Produção Própria; 2-20 demais) | Flag `_flag_lead_time_invalido` |
| `uf_origem` | 27 UFs do Brasil | Flag `_flag_uf_invalida` |
| `razao_social` | Não nula/vazia | Flag `_flag_razao_social_vazia` |

**Regras técnicas — food_lotes_producao:**

| Regra | Validação | Ação |
|---|---|---|
| `id_lote` PK | Não nulo, não duplicado | Remove |
| `sku` FK | Deve existir no catálogo unificado (Silvers de ecommerce + perecíveis) | Flag `_flag_sku_sem_catalogo` |
| `id_fornecedor` FK | Deve existir na Silver de fornecedores | Flag `_flag_fornecedor_orfao` |
| `status` | Enum: Ativo, Vencido, Recall | Flag `_flag_status_invalido` |
| `dt_validade` > `dt_fabricacao` | Validade invertida = chaos proposital | Flag `_flag_validade_invertida` |
| `temperatura_armazenamento_ideal` | Enum: Refrigerado, Ambiente, Congelado | Flag `_flag_temperatura_invalida` |
| `quantidade_produzida` > 0 | Quantidade zero ou negativa | Flag `_flag_quantidade_invalida` |

> Dados inválidos são **flagados, nunca descartados**. A decisão de uso fica para a camada Gold.

### Camada Gold — Sumarizações e KPIs de negócio

Lê os Deltas da Silver, aplica **regras de negócio** [planilha de regras das tabelas 'food_fornecedores' (F6-F10) e 'food_lotes_producao'(L6-L10)],
e grava em dois destinos simultâneos:

- **Delta Lake:** container `squad3/gold/` — fonte da verdade, versionada
- **SQL Server:** schema `squad3.gold_*` — pronto para consumo via Looker

**Modos de escrita:**
- **MERGE** (upsert por PK) para 8 tabelas acumulativas
- **OVERWRITE** / **TRUNCATE+INSERT** para 2 tabelas de alerta (L6, L10 fato), onde chaves antigas devem ser removidas entre execuções

**Políticas transversais:**
- Flags da Silver são confiadas (100% `false` em `_flag_sku_sem_catalogo` e `_flag_categoria_invalida` → sem revalidação)
- Auditoria: coluna `gold_processed_at` em todas as tabelas
- Partição Delta: por `ano` quando a coluna existe no schema
- SQL Server: DDL explícito com tipos otimizados (`decimal(10,2)` para percentuais, `datetime2` para timestamps, `nvarchar` com tamanhos realistas, PKs com índice clusterizado)

**10 tabelas Gold produzidas:**

| # | Tabela | Grão (PK) | Modo | Regra |
|---|---|---|---|---|
| 1 | `gold_fornecedor_lead_time_trimestre` | tipo_fornecedor + ano + trimestre | MERGE | F6 |
| 2 | `gold_fornecedor_ativos_categoria` | categoria | MERGE | F7 + F8 |
| 3 | `gold_fornecedor_distribuicao_uf` | uf_origem | MERGE | F9 |
| 4 | `gold_fornecedor_tempo_ativacao` | id_fornecedor | MERGE | F10 |
| 5 | `gold_lote_alerta_ativo_vencido` | id_lote | OVERWRITE | L6 |
| 6 | `gold_lote_volume_categoria_mes` | canal + categoria + ano + mes | MERGE | L7 |
| 7 | `gold_lote_recall_fornecedor_trimestre` | id_fornecedor + ano + trimestre | MERGE | L8 |
| 8 | `gold_lote_shelf_life_categoria` | categoria_pai | MERGE | L9 |
| 9 | `gold_lote_sem_estoque` | id_lote | OVERWRITE | L10 (fato) |
| 10 | `gold_fornecedor_canal_distribuicao` | id_fornecedor | MERGE | L10 (dimensão) |

**Decisões de modelagem (consolidadas no notebook 07 exploratório):**

| Regra | Decisão | Justificativa |
|---|---|---|
| F6 — Lead time por trimestre | Trimestre de `dt_fabricacao` do lote (não de `dt_cadastro`) | Interpretação (b) tem 300-700 lotes/célula vs. 1-5 fornecedores na (a) — KPI estável e acionável |
| F8 — Alerta de categoria sem fornecedor | Left join a partir da lista de categorias de referência | Garante que categorias com ZERO fornecedores apareçam na Gold com `flag_alerta_sem_ativo=true` |
| F10 — Tempo de ativação | Sem `flag_nunca_produziu`, sem tratamento de outlier | Exploração confirmou: todos os 60 fornecedores têm lote; zero negativos |
| L6 — Data de referência | Widget `data_referencia` com default `current_date()` | Permite backtest (ex: `'2025-06-30'`) sem alterar código |
| L6 — Severidade | Enum: crítico (>180d), alto (31-180d), moderado (≤30d) | 221/41/119 lotes respectivamente — segmentação acionável no Looker |
| L7 — Categoria híbrida | Coluna `canal` ('perecivel' / 'ecommerce') | Preserva 100% dos lotes (70% perecíveis + 30% e-commerce sem `categoria_pai`) |
| L8 — Significância estatística | Coluna extra: baixa_amostra (<3), amostra_media (3-9), amostra_boa (≥10) | Evita falso positivo (ex: fornecedor com 1 lote e 1 recall = 100% é irrelevante) |
| L9 — Janelas esperadas | Mantidas como valores da planilha de regras | Observação coincide 100% com o esperado — zero outlier |
| L10 — Modelo B (duas tabelas) | Fato (lotes) + dimensão (perfil fornecedor) | Perfil de distribuição vira contexto estratégico reutilizável em outras análises |
| L10 — Classificação de perfil | taxa_chegada_loja: ≥80% = loja_fisica; 20-80% = misto; <20% = atacado_b2b | Resultado: 9/11/40 fornecedores — distribuição binária confirma hipótese do canal B2B |

### Dependências entre squads (cascata de qualidade)

A Silver e a Gold da Squad 3 consomem Silvers de outras squads — nunca lêem do Raw em produção.
Isso segue o princípio de que cada camada confia na camada anterior.

| Tabela de referência | Responsável | Usada em | Status atual |
|---|---|---|---|
| `ecommerce_produtos` | Squad 1 | Validação de FK `sku` (Silver 06), catálogo de SKU e-commerce (Gold L7) | ✅ Silver disponível |
| `ecommerce_categorias` | Squad 1 | Validação de `categoria_fornecida` (Silver 06 e Gold F7+F8) | ✅ Silver disponível |
| `physical_produtos_pereciveis` | Squad 3 (outro membro) | Validação de SKU/categoria + enriquecimento `categoria_pai` (L7, L9) | ⚠️ Fallback Raw temporário |
| `food_estoque_lojas` | Squad 3 (outro membro) | Cruzamento L10 (lotes sem estoque) | ⚠️ Fallback Raw temporário |

**Fallback Silver→Raw aceito temporariamente:** quando a Silver de referência ainda não está publicada,
o notebook 07 (Gold) lê do Raw com log explícito (`⚠️ RAW — sem flags`). Quando as Silvers forem publicadas,
basta re-rodar — o notebook detecta automaticamente e passa a ler da Silver.

> ⚠️ Se qualquer uma dessas Silvers principais não existir, o notebook 06 falha explicitamente.
> Isso torna a dependência visível e evita falsos positivos nas flags.

---

## Estrutura dos notebooks

```
01_setup_adls.py                   → Conexão com o ADLS Gen2 e listagem dos arquivos
02_eda_fornecedores.py             → Análise exploratória de food_fornecedores
03_eda_lotes_producao.py           → Análise exploratória de food_lotes_producao
04_ingestao_sql.py                 → Validação de permissões e ingestão no SQL Server
05_ingestao_bronze.py              → Ingestão Bronze: raw (CSV) → Delta com auditoria, partição e append incremental
06_ingestao_silver.py              → Ingestão Silver: limpeza, tratamento, flags e merge por PK
07_exploracao_cruzamentos.py       → Exploração cruzada das Silvers (decisões de modelagem pré-Gold)
07_ingestao_gold.py                → Ingestão Gold: 10 tabelas sumarizadas em Delta + SQL Server
```

### Fluxo semanal (toda segunda-feira)

```
05_ingestao_bronze  →  06_ingestao_silver  →  07_ingestao_gold
     (append)              (merge)              (merge/overwrite + SQL Server)
```

O notebook `07_exploracao_cruzamentos` é **de uso único** (exploratório) — gerou as decisões de modelagem
documentadas na seção Gold acima. Não entra no fluxo semanal.

---

## Stack

- **Plataforma:** Databricks Serverless (Unity Catalog habilitado + Spark Connect)
- **Data Lake:** Azure Data Lake Gen2 — storage account `internshipdatalake`
- **Containers:** `raw` (CSVs originais, somente leitura) · `squad3` (Bronze, Silver e Gold em Delta)
- **Autenticação ADLS:** Service Principal (OAuth 2.0)
- **Formato de armazenamento:** Delta Lake (particionado por ano/mês)
- **Banco de dados:** Azure SQL Server — schema `squad3`
- **Conector SQL (dados):** `format("sqlserver")` nativo do Databricks Serverless
- **Conector SQL (DDL/MERGE):** `format("jdbc")` com `sessionInitStatement` (compatível com Spark Connect)

### Restrições do ambiente

Aprendizados técnicos coletados ao longo do desenvolvimento:

- `input_file_name()` bloqueado pelo Unity Catalog → usar `_metadata.file_path`
- `spark.conf.set()` bloqueado para configs Hadoop → `DeltaTable.merge()` indisponível, merge implementado via DataFrame (union + dedup + overwrite)
- `.cache()` / `.persist()` bloqueados no Serverless (`PERSIST TABLE is not supported`) → trabalhar sem cache; para volumes pequenos (~10k registros) o custo de recomputar é desprezível
- `pymssql` **trava o kernel** no Serverless (dependência nativa C incompatível) → substituído por JDBC puro via Spark Reader
- `spark._jvm.java.sql.DriverManager` não disponível no Spark Connect do Serverless → usar `spark.read.format("jdbc").option("sessionInitStatement", ...)` para executar DDL/MERGE/TRUNCATE
- `dbutils.fs.rm()` não funciona em paths ADLS com OAuth (`Invalid configuration value detected for fs.azure.account.key`) → sem efeito no fluxo da Gold; apenas limpezas de teste manuais ficam órfãs no bucket
- Spark JDBC Reader envolve a query em subquery (`SELECT * FROM (...) SPARK_GEN_TABLE`) → **nunca** usar `ORDER BY` dentro da query; ordenar com `.orderBy()` após `.load()`

### Observações sobre JDBC + SQL Server

A função utilitária `_execute_sql` do notebook 07 executa qualquer comando T-SQL arbitrário (DDL, MERGE, TRUNCATE)
usando `sessionInitStatement` do Spark JDBC Reader com uma query descartável (`SELECT 1 AS ok`) para satisfazer
o requisito de ResultSet. O comando real roda no init da conexão, antes do SELECT fake. Múltiplos comandos
são concatenados com `;` num único batch.

Esse padrão foi o único encontrado que funciona em Databricks Serverless sem Py4J e sem bibliotecas nativas
(`pymssql`, `pyodbc`).

---

## Como executar

### Pré-requisitos

1. Ter acesso ao workspace Databricks do programa
2. Arquivo `.env` configurado fora do repositório:
   ```
   /Workspace/Users/<seu-email>/.env
   ```
   Com as seguintes variáveis:
   ```
   ADLS_CLIENT_ID=
   ADLS_TENANT_ID=
   ADLS_CLIENT_SECRET=
   SQL_HOST=
   SQL_DATABASE=
   SQL_USERNAME=
   SQL_PASSWORD=
   ```
3. Service Principal com role `Storage Blob Data Contributor` nos containers `raw` e `squad3`
4. Usuário SQL com permissão de `CREATE TABLE`, `DROP TABLE` e `MERGE` no schema `squad3`
5. Para rodar o notebook 06: Silvers das Squads 1 e 3 (tabelas de referência) devem estar disponíveis
6. Para rodar o notebook 07 (Gold): Silvers das Squads 1 e 3 (preferencial) OU Raw correspondente (fallback automático)

### Ordem de execução

```
01 → 02 → 03 → 04                 (setup + EDA + SQL — executar uma vez)
07_exploracao_cruzamentos          (exploração — executada uma vez para gerar decisões)
05 → 06 → 07_ingestao_gold         (Bronze + Silver + Gold — executar semanalmente)
```

Cada notebook carrega as credenciais do `.env` de forma independente —
não é necessário rodar um antes do outro para carregar variáveis.

### Widget do notebook 07 (Gold)

- `data_referencia` (string, default vazio): data de referência para o alerta L6 (lotes ativos vencidos).
  Vazio = `current_date()`. Aceita formatos `YYYY-MM-DD` (ISO) ou `DD/MM/YYYY` (BR).
  Permite backtest histórico sem alterar código.

---

## Análise Exploratória — EDA

### food_fornecedores (60 registros | 10 colunas)

Cadastro completo dos fornecedores ativos que abastecem as lojas físicas e o e-commerce.

| Métrica | Valor |
|---|---|
| Total de fornecedores | 60 |
| Fornecedores ativos | 60 (100%) |
| Nulos em qualquer coluna | 0 |
| Estados de origem | 20 UFs |
| Categorias fornecidas | 17 categorias distintas |
| Lead time mínimo | 1 dia (Produção Própria) |
| Lead time máximo | 20 dias (Indústria) |
| Lead time médio geral | 10,8 dias |

**Distribuição por tipo de fornecedor:**
- Indústria: 30 fornecedores (50%)
- Distribuidor: 18 fornecedores (30%)
- Produção Própria (Loja): 4 fornecedores (6,7%)
- Produtor Rural: 4 fornecedores (6,7%)
- Indústria Frigorífica: 2 fornecedores (3,3%)
- Cooperativa Agrícola: 2 fornecedores (3,3%)

**Distribuição geográfica:**
- MA lidera com 7 fornecedores, seguido de SE, GO e DF com 4 cada.
- Boa diversificação regional — presença em todas as regiões do Brasil.

**Certificações mais comuns:**
- ANVISA: 44 fornecedores (73,3%) — obrigatória para alimentos
- ISO 22000: 37 fornecedores (61,7%) — gestão de segurança alimentar
- HACCP: 32 fornecedores (53,3%) — controle de pontos críticos
- SIF: 8 fornecedores (13,3%) — inspeção federal para produtos de origem animal
- Rastreabilidade GS1: 4 fornecedores (6,7%)
- Orgânico: 1 fornecedor (1,7%)

> ✅ Dataset de alta qualidade — zero nulos, todos os fornecedores ativos.
> Categorias com apenas 2-3 fornecedores (Laticínios & Ovos, Frios & Embutidos,
> Hortifruti Seco & Castanhas) representam possível risco de concentração de fornecimento.

---

### food_lotes_producao (10.882 registros | 8 colunas)

Rastreabilidade de lotes de produção vinculados aos SKUs e fornecedores.

| Métrica | Valor |
|---|---|
| Total de lotes | 10.882 |
| Nulos em qualquer coluna | 0 |
| Lotes Ativos | 1.907 (17,52%) |
| Lotes Vencidos | 8.811 (80,97%) |
| Lotes em Recall | 164 (1,51%) |
| Quantidade mínima por lote | 50 unidades |
| Quantidade máxima por lote | 4.998 unidades |
| Quantidade média por lote | 1.327 unidades |

**Distribuição por temperatura de armazenamento:**
- Refrigerado: 5.742 lotes (52,8%) — maior volume, produtos frescos
- Ambiente: 3.376 lotes (31%) — produtos de prateleira
- Congelado: 1.764 lotes (16,2%) — menor volume

**Validade média por temperatura:**
- Ambiente: 368 dias — produtos de prateleira com longa durabilidade
- Congelado: 236 dias — shelf life intermediário
- Refrigerado: 17,5 dias — produtos frescos com validade curta (laticínios, carnes, hortifruti)

**Principais fornecedores por volume total produzido:**
- Fornecedores 46 e 45 lideram com mais de 900k unidades e mais de 1.100 lotes cada.
- Fornecedores 8, 9, 10 e 11 têm poucos lotes (aproximadamente 162-167) mas altíssima produção por lote
  (aproximadamente 4.900 a 4.953 unidades) — perfil de produção industrial em escala.

> ⚠️ **Dado crítico:** 80,97% dos lotes estão com status Vencido — número elevado que
> indica necessidade de análise mais profunda (pode refletir histórico acumulado sem limpeza,
> ou problema real de gestão de validade).
>
> ⚠️ **Recall:** 164 lotes (1,51%) em status de Recall — dado sensível que exige
> acompanhamento e poderia ser cruzado com a tabela de fornecedores para identificar
> quais fornecedores concentram os recalls.

---

## Insights consolidados da Gold

Resumo dos alertas e insights disparados pelas 10 tabelas Gold após a última execução:

### Fornecedores (base: 60)

- **Categorias sem fornecedor ativo:** 0 — portfólio totalmente coberto
- **Categorias em risco de concentração (≤2 fornecedores ativos):** 4
  - Hortifruti Seco & Castanhas, Padaria Industrializada, Frios & Embutidos, Laticínios & Ovos
- **Distribuição por perfil de distribuição:**
  - `atacado_b2b`: 40 fornecedores (67%) — produzem mas não abastecem lojas físicas (taxa 0%)
  - `misto`: 11 fornecedores (18%) — taxa média 67%, atingem ~28 lojas
  - `loja_fisica`: 9 fornecedores (15%) — taxa média 97%, atingem todas as 29 lojas
- **Distribuição regional:** Nordeste concentra 42% dos fornecedores em 9 UFs; Sul tem apenas 7% em 2 UFs — possível perfil regional ou gap no eixo industrial SP/RJ

### Lotes (base: 10.882)

- **Alertas L6 — lotes `Ativo` com validade vencida:** 381
  - Crítico (>180 dias vencido): 221 lotes — média 572 dias, máximo 977 dias (2,7 anos)
  - Alto (31-180 dias): 41 lotes
  - Moderado (≤30 dias): 119 lotes
- **Lotes sem registro em estoque (L10):** 4.016 (36,9%)
  - 1.327 são `Ativo` → acionáveis para operações (não vencidos ainda)
  - 2.634 são `Vencido` → perda operacional já consumada
- **Recall por fornecedor (L8):** 118 combinações `(fornecedor × trimestre)` com pelo menos 1 recall
  - Em amostra confiável (≥3 lotes no trimestre): Azevedo Ltda. como fornecedor com indicação estatística mais forte de problema de qualidade
- **Shelf life por categoria (L9):** min/max observados batem **100%** com janelas esperadas — zero outlier em 7.508 lotes perecíveis

### Volume (base: 10.882 lotes, 14.448.410 unidades)

- **Canal perecível:** 7.634 lotes (70%) → 5.955.857 unidades (41% do volume) — muitos lotes pequenos, rotação alta
- **Canal e-commerce:** 3.248 lotes (30%) → 8.492.553 unidades (59% do volume) — poucos lotes grandes, shelf life longo
- Cada lote de e-commerce é **3,3× maior** que um lote perecível em volume médio

---

## Dashboards sugeridos para o Looker

Combinações cross-tabela que agregam valor imediato:

| Dashboard | Tabelas | Insight |
|---|---|---|
| **Radar de risco de categoria** | L7 (volume) + L9 (shelf life) + L6 (alertas) | Categorias com volume alto + shelf life curto + alertas = prioridade |
| **Matriz de risco de fornecedor** | L8 (recall) + L6 (vencidos) + L10 dim (perfil) | Fornecedor com recall alto E canal loja = risco direto no consumidor |
| **Operacional L10** | L10 fato filtrada por `status='Ativo'` | 1.327 lotes ativos sem distribuição = ação imediata |
| **Estratégico de canal** | L10 dim | Distribuição 67% atacado / 15% loja — informa decisões comerciais |
| **Capilaridade regional** | F9 (UF) + L10 dim (lojas atingidas) | Cruzar origem × alcance de distribuição |

---

## Observações de segurança

- Credenciais **nunca** hardcodadas nos notebooks
- Arquivo `.env` fora do repositório Git e listado no `.gitignore`
- Container `raw` é **somente leitura** — nenhuma escrita por parte da Squad 3
- Acesso restrito ao container `squad3` e schema SQL `squad3`
- Nenhuma estrutura compartilhada foi alterada
- Tabelas de referência de outras squads são apenas **lidas** da Silver delas para validação de FK (nunca escritas)
- DDL do SQL Server é **idempotente** (`IF OBJECT_ID() IS NULL`) — re-execuções não quebram tabelas existentes

---

## Débitos técnicos conhecidos

Pendências documentadas para resolução em sprints futuros:

1. **Silver de `physical_produtos_pereciveis`:** notebook do colega (`squad3_silver_pcsa.ipynb`) define `df_produtos_silver` mas não chama `salvar_silver_merge` — comunicado ao colega para correção. Enquanto não publica, o 07 lê do Raw com fallback automático.
2. **F10 — limitação histórica:** para fornecedores cadastrados antes de 2024-01-01, o `dias_ate_ativacao` reflete o início da série temporal de `food_lotes_producao`, não a ativação real. Métrica torna-se precisa à medida que novos fornecedores são cadastrados após essa data.

---

## Branch

`feature/henrique-ficht`
