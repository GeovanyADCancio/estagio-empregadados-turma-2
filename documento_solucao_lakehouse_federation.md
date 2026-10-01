# Solução de Acesso a Dados no Databricks Serverless via Lakehouse Federation

## Documento Técnico — Squad 2 (Kauan Fiabane & Leandro Reis)
Data: 30 de Setembro de 2026 (atualizado em 01 de Outubro de 2026)

---

## 1. Contexto e Problema

### 1.1 Ambiente
- **Cloud Provider:** AWS
- **Compute:** Serverless (CPU) — sem clusters clássicos disponíveis
- **Fonte de dados:** Azure Data Lake Storage Gen2 (ADLS)
  - Storage account: `internshipdatalake`
  - Container: `raw`
  - Caminho: `real-time-data/`
  - Arquivos: `ecommerce_produtos.parquet`, `ecommerce_categorias.parquet`
- **Banco de dados relacional:** Azure SQL Server
  - Host: `srv-database-intership.database.windows.net`
  - Database: `internshipDatabase`
  - Credenciais no arquivo `.env` (SQL_HOST, SQL_USERNAME, SQL_PASSWORD)

### 1.2 O Problema
O Databricks Serverless compute **bloqueia todo tráfego de rede externo**. O DNS resolve
domínios externos para um IP privado (`192.168.200.20` — sinkhole), impedindo:

1. **Azure SDK** (`ClientSecretCredential`, `DataLakeServiceClient`) — não consegue conectar ao ADLS
2. **JDBC direto** (`spark.read.format("sqlserver")`) — a conexão com o SQL Server falha silenciosamente
3. **REST APIs externas** — qualquer chamada para serviços fora da VPC da Databricks

### 1.3 Por que o código original falhava

O notebook `01_extracao_produtos_categorias` usava o Azure SDK para ler parquet do ADLS:

```python
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(
    account_url=f"https://internshipdatalake.dfs.core.windows.net",
    credential=credential,
)
```

No serverless, esse código falha porque o DNS resolve `internshipdatalake.dfs.core.windows.net`
para `192.168.200.20` (sinkhole), e a conexão TLS falha.

O notebook original tinha um fallback para dados mock (simulados), mas isso significava
trabalhar com dados falsos — não serve para análise real.

---

## 2. A Solução: Unity Catalog Lakehouse Federation

### 2.1 O que é Lakehouse Federation?

Lakehouse Federation é um recurso do Unity Catalog que permite acessar bancos de dados
externos (SQL Server, PostgreSQL, MySQL, etc.) através de **catálogos estrangeiros**
(foreign catalogs).

A diferença crucial: a conexão com o banco externo é **gerenciada pela infraestrutura
do Unity Catalog**, não pelo nó de compute. Isso significa que o bloqueio de rede do
serverless NÃO se aplica — o UC faz a conexão por você.

```
┌─────────────────────────────────────────────────┐
│                 Databricks                       │
│                                                  │
│  ┌──────────┐    ┌──────────────────┐             │
│  │ Serverless │    │ Unity Catalog    │             │
│  │ Compute   │───▶│ (Infraestrutura) │───▶ SQL Server│
│  │ (bloqueio │    │ (sem bloqueio)   │    (Azure)   │
│  │  de rede) │    │                  │             │
│  └──────────┘    └──────────────────┘             │
└─────────────────────────────────────────────────┘
```

### 2.2 Passo a passo da implementação

#### Passo 1: Criar a conexão no Unity Catalog

Uma **connection** é a configuração de como o UC se conecta ao banco externo.
Criamos via Databricks SDK (Python):

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
from dotenv import load_dotenv
import os

w = WorkspaceClient()

# Ler credenciais do .env (sem expor no código)
load_dotenv(".env", override=True)
host = os.getenv("SQL_HOST", "").strip('"\'')
user = os.getenv("SQL_USERNAME", "").strip('"\'')
password = os.getenv("SQL_PASSWORD", "").strip('"\'')

# Criar a conexão
result = w.connections.create(
    name="sqlserver_internship",
    connection_type=ConnectionType.SQLSERVER,
    options={
        "host": host,
        "port": "1433",
        "user": user,
        "password": password,
        "trustServerCertificate": "false"
    }
)
```

**Importante:** As opções `database` e `encrypt` NÃO são suportadas na connection.
Elas vão no foreign catalog (Passo 2). As opções válidas para SQL Server são:
`host`, `port`, `user`, `password`, `trustServerCertificate`,
`userProvidedServerCertificate`, `applicationIntent`.

#### Passo 2: Criar o catálogo estrangeiro

Um **foreign catalog** é um catálogo no Unity Catalog que aponta para um banco de dados
específico no servidor externo:

```python
result = w.catalogs.create(
    name="sqlserver_catalog",
    connection_name="sqlserver_internship",
    options={"database": "internshipDatabase"}
)
```

#### Passo 3: Ler dados via Spark

Agora qualquer tabela do SQL Server é acessível como se fosse uma tabela do Unity Catalog:

```python
# Ler via PySpark
df_produtos = spark.table("sqlserver_catalog.squad2.ecommerce_produtos")

# Ou via SQL
spark.sql("SELECT * FROM sqlserver_catalog.dbo.ecommerce_clientes")
```

### 2.3 Estrutura criada

```
Unity Catalog
└── Connection: sqlserver_internship (TYPE SQLSERVER)
    └── Foreign Catalog: sqlserver_catalog (database: internshipDatabase)
        ├── schema: dbo
        │   ├── ecommerce_produtos (0 linhas — esvaziada por JDBC)
        │   ├── ecommerce_categorias (1.620 linhas)
        │   ├── ecommerce_clientes (7.656 linhas)
        │   ├── ecommerce_enderecos
        │   ├── ecommerce_itens_pedido
        │   ├── ecommerce_pedidos
        │   └── ecommerce_rastreamento
        ├── schema: squad1
        │   └── (tabelas do Squad 1)
        ├── schema: squad2
        │   ├── ecommerce_produtos (2.874 linhas)
        │   ├── ecommerce_categorias (135 linhas)
        │   ├── ecommerce_clientes (0 linhas — esvaziada por JDBC)
        │   └── ...
        └── schema: squad3
            └── (tabelas do Squad 3)
```

---

## 3. Limitações Descobertas (IMPORTANTE!)

### 3.1 Lakehouse Federation é somente leitura para SQL Server

O catálogo estrangeiro `sqlserver_catalog` **não permite escrita**.
Tentar gravar via `saveAsTable` retorna o erro:

```
UNAUTHORIZED_ACCESS: Only READ credentials can be retrieved for foreign tables.
SQLSTATE: 42501
```

### 3.2 JDBC no serverless — ATUALIZADO (01/10/2026)

**Descoberta importante:** Após testes em 01/10/2026, confirmamos que o JDBC
de leitura E escrita FUNCIONA no serverless. O problema anterior (tabelas truncadas)
ocorreu provavelmente por DataFrame vazio — o `mode("overwrite")` trunca a tabela
primeiro e depois tenta inserir; se não há dados, a tabela fica vazia.

**Solução — função `escrever_tabela_jdbc()`:**
1. Verifica se o DataFrame NÃO está vazio (count > 0) antes de escrever
2. Escreve via JDBC com mode("overwrite") ou mode("append")
3. Lê de volta para confirmar que os dados foram gravados

**Proteção de leitura — função `ler_tabela_jdbc()`:**
1. Usa .option("readOnly", "true") no driver JDBC
2. Função bloquear_escrita_jdbc() levanta PermissionError se chamada
3. A função só expõe spark.read — não há caminho de spark.write

**Resumo do que funciona no serverless:**

| Operação | Funciona? | Observação |
| --- | --- | --- |
| JDBC read (spark.read.format sqlserver) | Sim | Testado com 4 tabelas, 90K+ linhas |
| JDBC write (spark.write.format sqlserver) | Sim | Com função de segurança escrever_tabela_jdbc() |
| Lakehouse Federation (read) | Sim | Gerenciado pelo Unity Catalog |
| Lakehouse Federation (write) | Nao | Catalogo estrangeiro e somente leitura |
| Azure SDK (ADLS) | Nao | DNS sinkhole bloqueia conexao |

### 3.3 Solução para escrita: tabela Delta no catálogo workspace

Como não podemos escrever no SQL Server a partir do serverless, gravamos em uma
tabela Delta no catálogo `workspace.default`:

```python
df_clientes.write \
    .mode("overwrite") \
    .saveAsTable("workspace.default.ecommerce_clientes")
```

Tabelas Delta no catálogo `workspace` funcionam perfeitamente no serverless e ficam
acessíveis para todos os notebooks do projeto.

---

## 4. Alterações nos Notebooks

### 4.1 Notebook 01 — `01_extracao_produtos_categorias`

**Antes:** ~80 linhas usando Azure SDK + fallback para dados mock

**Depois:** 3 linhas usando Lakehouse Federation

```python
# Leitura direta via Spark SQL — funciona no serverless!
df_produtos = spark.table("sqlserver_catalog.squad2.ecommerce_produtos")
df_categorias = spark.table("sqlserver_catalog.dbo.ecommerce_categorias")
df_clientes = spark.table("sqlserver_catalog.dbo.ecommerce_clientes")

# Validação
print(f"Produtos:  {df_produtos.count()} linhas")   # 2.874
print(f"Categorias: {df_categorias.count()} linhas") # 1.620
print(f"Clientes:  {df_clientes.count()} linhas")   # 7.656
```

**Por que schemas diferentes?**
- `squad2.ecommerce_produtos`: 2.874 linhas (dbo está vazia — esvaziada por JDBC)
- `dbo.ecommerce_categorias`: 1.620 linhas (mais completa que squad2 que tem 135)
- `dbo.ecommerce_clientes`: 7.656 linhas (squad2 está vazia — esvaziada por JDBC)

### 4.2 Notebook 02 — `02_data_load_sqlserver.ipynb`

**Antes:** 8 células com setup JDBC, escrita via `format("sqlserver")` e leitura JDBC

**Depois:** 4 células simplificadas

1. `%run ./01_extracao_produtos_categorias` — carrega dados do notebook 01
2. Comentário explicando a abordagem
3. Escrita em tabela Delta: `workspace.default.ecommerce_clientes`
4. Verificação: lê a tabela Delta e exibe os dados

### 4.3 Notebook 03_auditoria — `03_auditoria_data_quality` (atualizado 01/10/2026)

**Antes:** 11 células com fragmentos soltos e código duplicado

**Depois:** 5 células limpas:
1. `%run ./01_extracao_produtos_categorias` — carrega 4 DataFrames
2. Função `analisar_qualidade_dataframe()` — auditoria das 4 tabelas
3. Markdown — explicação da análise de KPIs
4. Comparação de KPIs (1x vs 2x vs 3x sem dedup) — demonstra inflação
5. Inflação por status + conclusão

### 4.4 Notebook 03_analise_clientes — `03_analise_clientes.ipynb` (atualizado 01/10/2026)

**Antes:** Usava rglob para encontrar o .env e JDBC sem proteção

**Depois:** 6 células com .env dinâmico via `dbutils.notebook.entry_point`:
1. .env dinâmico + credenciais
2. Funções `ler_tabela_jdbc()` (readOnly=true) e `bloquear_escrita_jdbc()`
3. Leitura de `dbo.ecommerce_clientes` (7.656 linhas)
4. Schema + amostra dos dados
5. Volume de dados
6. Integridade da chave primária

### 4.5 Notebook 05 — `05_filtragem_pedidos_controle` (criado 01/10/2026)

Filtragem de `ecommerce_pedidos` com controle de estado por hash SHA-256.
Lê 90.335 pedidos via Lakehouse Federation, calcula hash de cada linha, compara com
a tabela de controle `workspace.default.controle_pedidos`, e processa apenas
registros novos/alterados. Idempotência validada: 2ª execução = 0 registros novos.

### 4.6 Notebook 06 — `06_acesso_jdbc_env_dinamico` (criado 01/10/2026)

Acesso JDBC ao SQL Server com .env dinâmico (solução do colega) + comparação com
Lakehouse Federation. Inclui função `escrever_tabela_jdbc()` com 3 camadas de
segurança: verifica count>0, escreve, lê de volta para confirmar.

---

## 5. Lições Aprendidas

### 5.1 No serverless, NUNCA use:
- ❌ Azure SDK (ClientSecretCredential, DataLakeServiceClient) — DNS sinkhole bloqueia
- ❌ JDBC mode("overwrite") com DataFrame vazio — trunca a tabela sem escrever
- ❌ Qualquer chamada de rede externa direta (REST APIs)

### 5.2 No serverless, SEMPRE use:
- ✅ Lakehouse Federation para LER de bancos externos (sem credenciais no código)
- ✅ JDBC com `ler_tabela_jdbc()` (readOnly=true) como alternativa à Lakehouse Federation
- ✅ JDBC com `escrever_tabela_jdbc()` (verifica count>0 + readback) para escrever no SQL Server
- ✅ Tabelas Delta no catálogo `workspace` para gravar dados localmente
- ✅ `dbutils.notebook.entry_point` para resolver o caminho do `.env` dinamicamente
- ✅ Controle de estado por hash para evitar reprocessamento e inflação de KPIs

### 5.3 Fluxo de dados correto no serverless

```
SQL Server (Azure) ──Ler──▶ Unity Catalog (foreign catalog) ──▶ Spark DataFrame
                  ──Ler──▶ JDBC (ler_tabela_jdbc, readOnly=true)  ─┘
                                                                      │
                                                                      ▼
                                              Controle de estado por hash (SHA-256)
                                                                      │
                                                                      ▼
                                              Tabela Delta (workspace.default)
                                                                      │
                                                                      ▼
                                                          Análise / EDA / Relatórios
```

### 5.4 Descobertas da Semana 2 (01/10/2026)

1. JDBC funciona no serverless — leitura e escrita testadas e validadas
2. .env dinâmico — `dbutils.notebook.entry_point` resolve o caminho do notebook
3. Controle de estado por hash — evita reprocessamento e inflação de KPIs
4. JDBC e Lakehouse Federation retornam dados idênticos — ambos são válidos
5. DataFrame vazio + overwrite = perda de dados — função `escrever_tabela_jdbc()` previne
6. dbo vs squad2 — tabelas em schemas diferentes têm dados diferentes:
   - squad2.ecommerce_pedidos: 90.335 linhas (schema completo com dt_previsao_entrega)
   - dbo.ecommerce_clientes: 7.656 linhas (mais completa que squad2 que tem 13)
   - squad2.ecommerce_produtos: 2.874 linhas (dbo está vazia)

---

## 6. Comandos SQL Úteis

```sql
-- Listar conexões UC
SHOW CONNECTIONS;

-- Listar catálogos
SHOW CATALOGS;

-- Listar schemas do catálogo estrangeiro
SHOW SCHEMAS IN sqlserver_catalog;

-- Listar tabelas de um schema
SHOW TABLES IN sqlserver_catalog.squad2;
SHOW TABLES IN sqlserver_catalog.dbo;

-- Ler dados
SELECT * FROM sqlserver_catalog.squad2.ecommerce_produtos LIMIT 5;
SELECT * FROM sqlserver_catalog.dbo.ecommerce_clientes LIMIT 5;
SELECT * FROM workspace.default.ecommerce_clientes LIMIT 5;
```

---

## 7. Estrutura Final dos Notebooks

```
estagio-empregadados-turma-2/
├── .env                              # Credenciais (ADLS + SQL Server)
├── documento_solucao_lakehouse_federation.md  # Este documento
├── 00_setup_config_le                # Setup original (não mais necessário)
├── 01_extracao_produtos_categorias    # Lê 4 tabelas via Lakehouse Federation
│   └── df_produtos, df_categorias, df_clientes, df_pedidos
├── 02_data_load_sqlserver.ipynb      # Grava em Delta (workspace.default)
├── 03_auditoria_data_quality         # Análise de qualidade + KPIs (5 células)
├── 03_analise_clientes.ipynb         # EDA de clientes via JDBC + .env dinâmico
├── 04_carga_sqlserver                # Carga original (precisa de atualização)
├── 05_filtragem_pedidos_controle     # Filtragem com controle de estado por hash
└── 06_acesso_jdbc_env_dinamico       # JDBC + .env dinâmico + comparação com Lakehouse
```

### Tabelas no Unity Catalog (workspace.default)

| Tabela | Função |
| --- | --- |
| `controle_pedidos` | Controle de estado: id_pedido + hash_registro + dt_processamento |
| `pedidos_filtrados` | Pedidos processados (append incremental, só novos/alterados) |
| `ecommerce_clientes` | Cópia Delta dos clientes (gravada pelo notebook 02) |

---

## 8. Glossário

| Termo | Significado |
| --- | --- |
| Serverless compute | Compute gerenciado sem clusters fixos — bloqueia rede externa |
| Unity Catalog (UC) | Serviço de governança de dados do Databricks |
| Lakehouse Federation | Recurso do UC para acessar bancos externos via catálogos estrangeiros |
| Connection (UC) | Configuração de conexão com banco externo (host, credenciais) |
| Foreign Catalog | Catálogo UC que mapeia para um database externo |
| Storage Credential | Credencial de acesso a storage externo no UC |
| Delta table | Formato de tabela nativa do Databricks (parquet + transaction log) |
| Sinkhole DNS | DNS que resolve domínios externos para IP inválido (bloqueio) |
| JDBC | Java Database Connectivity — protocolo de conexão com bancos relacionais |

---

*Documento gerado por Genie Code — Databricks Assistant*
*Última atualização: 01 de Outubro de 2026*