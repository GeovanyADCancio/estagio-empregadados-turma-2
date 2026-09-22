# Squad 3 — Batch | Henrique Ficht

Notebooks desenvolvidos durante o programa de estágio **estagio-empregadados-turma-2**,
referentes às tabelas sob responsabilidade do Squad 3 (Batch).

---

## Tabelas sob responsabilidade

| Tabela | Fonte | Destino SQL Server |
|---|---|---|
| `food_estoque_lojas` | `abfss://raw@internshipdatalake.dfs.core.windows.net/batch-data/` | `squad3.food_estoque_lojas` |
| `food_avaliacoes_produto` | `abfss://raw@internshipdatalake.dfs.core.windows.net/batch-data/` | `squad3.food_avaliacoes_produto` |

---

## Estrutura dos notebooks

```
01_setup_adls.py         → Conexão com o ADLS Gen2 e listagem dos arquivos
02_eda_estoque.py        → Análise exploratória de food_estoque_lojas
03_eda_avaliacoes.py     → Análise exploratória de food_avaliacoes_produto
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

## Principais achados — EDA

### food_estoque_lojas (736.020 registros)

| Métrica | Valor |
|---|---|
| Lojas | 30 (id 1 a 30) |
| SKUs por loja | 25.380 (distribuição uniforme) |
| Nulos em `id_lote` | 533.509 (72% dos registros) |
| Itens abaixo do estoque mínimo | 60.316 (8,2%) |
| Quantidade mínima registrada | -240 (valores negativos presentes) |
| Período dos snapshots | Jan/2024 a Set/2026 (quinzenal) |

> ⚠️ `id_lote` apresenta alto índice de nulos — itens sem rastreabilidade de lote.
> Valores negativos em `quantidade_disponivel` indicam ajustes contábeis ou inconsistências operacionais.

### food_avaliacoes_produto (372.199 registros)

| Métrica | Valor |
|---|---|
| Nulos em `id_pedido` | 21.067 (coincide com avaliações não verificadas) |
| Notas fora do range esperado (1-5) | 1.843 registros (notas -1 e 6) |
| Avaliações verificadas | 351.132 (94,3%) |
| Nota mais frequente | 5 — representa 54,76% das avaliações |
| Melhor SKU avaliado | `sorvetes-benjerrys-2l-napolitano` (média 4,91) |
| Pior SKU avaliado | `sorvetes-kibon-1l-picoleun` (média 3,07) |
| Período | Dez/2023 a Mai/2026 (crescimento consistente) |

> ⚠️ Notas -1 e 6 são dados sujos e deverão ser tratados em camada Silver futuramente.
> Avaliações não verificadas não possuem `id_pedido` associado.

---

## Observações de segurança

- Credenciais **nunca** hardcodadas nos notebooks
- Arquivo `.env` fora do repositório Git e listado no `.gitignore`
- Acesso restrito às tabelas do schema `squad3`
- Nenhuma estrutura compartilhada foi alterada

---

## Branch

`feature/henrique-ficht`
