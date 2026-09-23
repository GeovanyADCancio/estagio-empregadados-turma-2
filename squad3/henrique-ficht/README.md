# Squad 3 — Batch | Henrique Ficht

Notebooks desenvolvidos durante o programa de estágio **estagio-empregadados-turma-2**,
referentes às tabelas sob responsabilidade do Squad 3 (Batch).

---

## Tabelas sob responsabilidade

| Tabela | Fonte | Destino SQL Server |
|---|---|---|
| `food_fornecedores` | `abfss://raw@internshipdatalake.dfs.core.windows.net/batch-data/` | `squad3.food_fornecedores` |
| `food_lotes_producao` | `abfss://raw@internshipdatalake.dfs.core.windows.net/batch-data/` | `squad3.food_lotes_producao` |

---

## Estrutura dos notebooks

```
01_setup_adls.py         → Conexão com o ADLS Gen2 e listagem dos arquivos
02_eda_fornecedores.py   → Análise exploratória de food_fornecedores
03_eda_lotes_producao.py → Análise exploratória de food_lotes_producao
04_ingestao_sql.py       → Validação de permissões e ingestão no SQL Server
```

---

## Stack

- **Plataforma:** Databricks Free Edition (Serverless)
- **Data Lake:** Azure Data Lake Gen2 — storage account `internshipdatalake`
- **Container:** `raw` / pasta `batch-data/`
- **Autenticação ADLS:** Service Principal (OAuth 2.0)
- **Banco de dados:** Azure SQL Server — schema `squad3`
- **Conector SQL:** `format("sqlserver")` nativo do Databricks Serverless

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

### Ordem de execução

Execute os notebooks na ordem numérica:

```
01 → 02 → 03 → 04
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
- Fornecedores 8, 9, 10 e 11 têm poucos lotes (~162-167) mas altíssima produção por lote
  (~4.900 a 4.953 unidades) — perfil de produção industrial em escala.

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
- Acesso restrito às tabelas do schema `squad3`
- Nenhuma estrutura compartilhada foi alterada

---

## Branch

`feature/henrique-ficht`
