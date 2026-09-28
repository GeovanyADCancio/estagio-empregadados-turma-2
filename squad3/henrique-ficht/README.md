# Squad 3 — Batch | Henrique Ficht

Notebooks desenvolvidos durante o programa de estágio **estagio-empregadados-turma-2**,
referentes às tabelas sob responsabilidade do Squad 3 (Batch).

---

## Tabelas sob responsabilidade

| Tabela | Fonte (Raw) | Bronze | Silver |
|---|---|---|---|
| `food_fornecedores` | `raw/batch-data/` | `squad3/bronze/food_fornecedores` | `squad3/silver/food_fornecedores` |
| `food_lotes_producao` | `raw/batch-data/` | `squad3/bronze/food_lotes_producao` | `squad3/silver/food_lotes_producao` |

---

## Arquitetura Medalhão

```
Raw (CSV)  →  Bronze (Delta)  →  Silver (Delta)  →  Gold (futuro)
                 append             merge (upsert)
                 semanal            semanal
```

### Camada Bronze — Ingestão crua com auditoria

Lê os CSVs históricos do container `raw` e salva em Delta no container `squad3/bronze/`,
adicionando colunas de auditoria e particionamento.

- **Modo:** `append` (acumulativo — cada execução adiciona um snapshot)
- **Auditoria:** `bronze_ingested_at` (timestamp da ingestão), `bronze_source_file` (path do CSV)
- **Partição:** `ano`, `mes` (extraídos de `dt_cadastro` / `dt_fabricacao`)
- **Frequência:** semanal (toda segunda-feira)

### Camada Silver — Limpeza, tratamento e validação

Lê os Deltas da Bronze, aplica regras técnicas da planilha de regras (linhas 117–136),
e salva em Delta no container `squad3/silver/`.

- **Modo:** `merge` (upsert por PK — implementado via DataFrame por restrição do Unity Catalog)
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
| `categoria_fornecida` | Validada contra `ecommerce_categorias` e `physical_produtos_pereciveis` (dinâmico) | Flag `_flag_categoria_invalida` |
| `tipo_fornecedor` | Lista permitida (7 tipos) | Flag `_flag_tipo_invalido` |
| `lead_time_dias` > 0 | Range por tipo (1-3 Produção Própria; 2-20 demais) | Flag `_flag_lead_time_invalido` |
| `uf_origem` | 27 UFs do Brasil | Flag `_flag_uf_invalida` |
| `razao_social` | Não nula/vazia | Flag `_flag_razao_social_vazia` |

**Regras técnicas — food_lotes_producao:**

| Regra | Validação | Ação |
|---|---|---|
| `id_lote` PK | Não nulo, não duplicado | Remove |
| `sku` FK | Deve existir no catálogo unificado (ecommerce + perecíveis) | Flag `_flag_sku_sem_catalogo` |
| `id_fornecedor` FK | Deve existir na Silver de fornecedores | Flag `_flag_fornecedor_orfao` |
| `status` | Enum: Ativo, Vencido, Recall | Flag `_flag_status_invalido` |
| `dt_validade` > `dt_fabricacao` | Validade invertida = chaos proposital | Flag `_flag_validade_invertida` |
| `temperatura_armazenamento_ideal` | Enum: Refrigerado, Ambiente, Congelado | Flag `_flag_temperatura_invalida` |
| `quantidade_produzida` > 0 | Quantidade zero ou negativa | Flag `_flag_quantidade_invalida` |

> Dados inválidos são **flagados, nunca descartados**. A decisão de uso fica para a camada Gold.

---

## Estrutura dos notebooks

```
01_setup_adls.py         → Conexão com o ADLS Gen2 e listagem dos arquivos
02_eda_fornecedores.py   → Análise exploratória de food_fornecedores
03_eda_lotes_producao.py → Análise exploratória de food_lotes_producao
04_ingestao_sql.py       → Validação de permissões e ingestão no SQL Server
05_ingestao_bronze.py    → Ingestão Bronze: raw (CSV) → Delta com auditoria e partição
06_ingestao_silver.py    → Ingestão Silver: limpeza, tratamento, flags e merge por PK
```

### Fluxo semanal (toda segunda-feira)

```
05_ingestao_bronze  →  06_ingestao_silver
     (append)              (merge)
```

---

## Stack

- **Plataforma:** Databricks Serverless (Unity Catalog habilitado)
- **Data Lake:** Azure Data Lake Gen2 — storage account `internshipdatalake`
- **Containers:** `raw` (CSVs originais) · `squad3` (Bronze e Silver em Delta)
- **Autenticação ADLS:** Service Principal (OAuth 2.0)
- **Formato de armazenamento:** Delta Lake (particionado por ano/mês)
- **Banco de dados:** Azure SQL Server — schema `squad3`
- **Conector SQL:** `format("sqlserver")` nativo do Databricks Serverless

### Restrições do ambiente

- `input_file_name()` bloqueado pelo Unity Catalog → usar `_metadata.file_path`
- `spark.conf.set()` bloqueado para configs Hadoop → `DeltaTable.merge()` indisponível, merge implementado via DataFrame (union + dedup + overwrite)

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

### Ordem de execução

```
01 → 02 → 03 → 04          (setup + EDA + SQL — executar uma vez)
05 → 06                     (Bronze + Silver — executar semanalmente)
```

Cada notebook carrega as credenciais do `.env` de forma independente —
não é necessário rodar um antes do outro para carregar variáveis.

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
  (aproximadamente  4.900 a 4.953 unidades) — perfil de produção industrial em escala.

> ⚠️ **Dado crítico:** 80,97% dos lotes estão com status Vencido — número elevado que
> indica necessidade de análise mais profunda (pode refletir histórico acumulado sem limpeza,
> ou problema real de gestão de validade).
>
> ⚠️ **Recall:** 164 lotes (1,51%) em status de Recall — dado sensível que exige
> acompanhamento e poderia ser cruzado com a tabela de fornecedores para identificar
> quais fornecedores concentram os recalls.

---

## Observações de segurança

- Credenciais **nunca** hardcodadas nos notebooks
- Arquivo `.env` fora do repositório Git e listado no `.gitignore`
- Acesso restrito ao container `squad3` e schema SQL `squad3`
- Nenhuma estrutura compartilhada foi alterada
- Tabelas de referência de outras squads são apenas **lidas** para validação de FK (nunca escritas)

---

## Branch

`feature/henrique-ficht`
