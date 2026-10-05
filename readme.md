# README — Integração entre Azure Data Lake Gen2 e PySpark (Databricks)

## 1. Objetivo

Este notebook demonstra um fluxo simples de **Engenharia de Dados + EDA (Exploratory Data Analysis)** utilizando:

- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Service Principal do Microsoft Entra ID
- Python e `python-dotenv`
- Azure SDK
- Apache Spark / PySpark
- Arquivos CSV armazenados no Data Lake

Os dados analisados são:

- `ecommerce_pedidos.csv`
- `ecommerce_clientes.csv`

---

## 2. Instalação das bibliotecas

```python
%pip install azure-storage-file-datalake azure-identity pandas python-dotenv
dbutils.library.restartPython()
```

Instala as bibliotecas necessárias para:

- `azure-storage-file-datalake`: acessar o ADLS Gen2.
- `azure-identity`: autenticação utilizando Service Principal.
- `python-dotenv`: carregar variáveis do arquivo `.env`.
- `pandas`: suporte para manipulação de dados, quando necessário.

O `restartPython()` reinicia o ambiente Python do Databricks para garantir que as bibliotecas instaladas sejam reconhecidas.

---

# 3. Carregamento das variáveis do `.env`

```python
load_dotenv()
```

O arquivo `.env` armazena as credenciais e configurações utilizadas na conexão:

```text
CLIENT_ID
CLIENT_SECRET
TENANT_ID
STORAGE_ACCOUNT
CONTAINER
```

Depois, as variáveis são recuperadas com:

```python
os.getenv("CLIENT_ID")
```

Uma validação verifica se todas as informações necessárias foram carregadas.

### Por que usar `.env`?

Evita colocar credenciais diretamente no código-fonte.

> **Importante:** o arquivo `.env` não deve ser versionado no Git. Inclua `.env` no `.gitignore`.

---

# 4. Autenticação com Service Principal

```python
credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)
```

Aqui é criada uma credencial utilizando um **Service Principal** do Microsoft Entra ID.

Essa identidade permite que a aplicação se autentique no Azure sem utilizar uma conta de usuário diretamente.

---

# 5. Conexão com o Azure Data Lake

```python
service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=credential
)
```

Cria o cliente responsável pela comunicação com o **Azure Data Lake Storage Gen2**.

Em seguida:

```python
filesystem_client = service_client.get_file_system_client(CONTAINER)
```

Define qual container será utilizado.

---

# 6. Listagem dos arquivos do Data Lake

```python
filesystem_client.get_paths(path="batch-data")
```

Lista os arquivos existentes dentro da pasta:

```text
CONTAINER/
└── batch-data/
    ├── ecommerce_pedidos.csv
    └── ecommerce_clientes.csv
```

O código identifica se cada item é um arquivo ou diretório e exibe essa informação.

---

# 7. Localização do `.env` no Databricks

Uma segunda etapa tenta localizar explicitamente o `.env` no diretório associado ao notebook:

```python
notebook_path = os.path.dirname(os.path.abspath(sys.argv[0]))
env_path = os.path.join(notebook_path, ".env")
```

Depois:

```python
load_dotenv(dotenv_path=env_path)
```

A ideia é garantir que o código procure o `.env` em um caminho conhecido.

Caso `CLIENT_ID` não seja encontrado, o código ainda tenta utilizar:

```python
load_dotenv()
```

Por fim, uma validação interrompe a execução caso alguma variável esteja ausente.

---

# 8. Função para leitura dos CSVs

A função:

```python
def ler_csv_adls(caminho_arquivo):
```

centraliza a leitura dos arquivos armazenados no ADLS.

Primeiro é construída a URL:

```python
url_adls = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{caminho_arquivo}"
```

O protocolo `abfss://` é utilizado pelo Spark para acessar o **Azure Data Lake Storage Gen2**.

---

# 9. Configuração da autenticação OAuth no Spark

A função configura o Spark para utilizar OAuth:

```python
.option("fs.azure.account.auth.type", "OAuth")
```

Também informa o provider:

```python
.option(
    "fs.azure.account.oauth.provider.type",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
```

E fornece as informações do Service Principal:

```python
.option("fs.azure.account.oauth2.client.id", CLIENT_ID)
.option("fs.azure.account.oauth2.client.secret", CLIENT_SECRET)
.option("fs.azure.account.oauth2.client.endpoint", endpoint_microsoft)
```

Assim, o Spark consegue autenticar diretamente no ADLS.

---

# 10. Leitura dos pedidos

```python
df_pedidos = ler_csv_adls("batch-data/ecommerce_pedidos.csv")
```

O arquivo de pedidos é carregado para um **Spark DataFrame**.

As opções utilizadas na leitura são:

```python
.option("header", "true")
.option("inferSchema", "true")
```

Isso significa que:

- A primeira linha contém os nomes das colunas.
- O Spark tenta identificar automaticamente os tipos dos dados.

---

# 11. Visualização inicial dos dados

```python
display(df_pedidos)
```

Exibe os registros do DataFrame no Databricks.

Essa etapa permite verificar rapidamente a estrutura e o conteúdo dos dados.

---

# 12. Estatísticas das colunas de valor

```python
df_pedidos.select(
    "valor_total",
    "valor_frete"
).summary()
```

Calcula estatísticas descritivas das colunas:

- `count`
- `mean`
- `stddev`
- `min`
- `max`

É uma análise inicial dos valores financeiros dos pedidos.

---

# 13. Análise dos status dos pedidos

Primeiro é calculado o número total de registros:

```python
total_registros = df_pedidos.count()
```

Depois os pedidos são agrupados por status:

```python
df_pedidos.groupBy("status_pedido")
```

A quantidade de pedidos é calculada e o percentual é obtido:

```python
round((col("quantidade") / total_registros) * 100, 2)
```

O resultado permite entender a distribuição dos pedidos entre os diferentes status.

---

# 14. Período dos pedidos

```python
df_pedidos.select(
    min("dt_pedido"),
    max("dt_pedido")
)
```

Identifica:

- Data do primeiro pedido.
- Data do último pedido.

Isso ajuda a entender o período coberto pela base.

---

# 15. Cálculo do prazo estimado

```python
datediff(
    col("dt_previsao_entrega"),
    col("dt_pedido")
)
```

Calcula a diferença, em dias, entre:

```text
Data prevista de entrega
-
Data do pedido
```

O resultado é armazenado na coluna:

```text
dias_prazo_estimado
```

Depois são calculadas estatísticas sobre essa nova coluna.

---

# 16. Verificação de valores nulos

```python
df_pedidos.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_pedidos.columns
])
```

Conta quantos valores `NULL` existem em cada coluna.

Essa é uma verificação importante de **qualidade dos dados** antes de realizar análises ou transformações posteriores.

---

# 17. Leitura da tabela de clientes

```python
df_clientes = ler_csv_adls(
    "batch-data/ecommerce_clientes.csv"
)
```

O segundo arquivo CSV é carregado utilizando a mesma função criada anteriormente.

Isso evita repetir a lógica de conexão e leitura.

---

# 18. Validação de integridade: pedidos x clientes

O código identifica primeiro o cliente com maior quantidade de pedidos:

```python
df_pedidos.groupBy("id_cliente")     .agg(count("*").alias("total_pedidos"))     .orderBy(col("total_pedidos").desc())     .first()
```

Depois verifica se esse `id_cliente` realmente existe na tabela de clientes.

Caso exista, seus dados são recuperados:

```python
nome
sobrenome
email
```

### Objetivo

Verificar a integridade do relacionamento:

```text
Pedidos.id_cliente
        ↓
Clientes.id_cliente
```

Se o ID não existir na tabela de clientes, temos um possível **registro órfão**.

---

# 19. Análise de recorrência

O código agrupa os pedidos por cliente:

```python
df_pedidos.groupBy("id_cliente")     .agg(count("*").alias("total_pedidos"))
```

Assim, cada cliente passa a ter a quantidade total de pedidos realizados.

Exemplo:

```text
id_cliente | total_pedidos
-----------|--------------
001        | 1
002        | 3
003        | 2
```

---

# 20. Métricas de recorrência

São calculados:

```python
min("total_pedidos")
max("total_pedidos")
avg("total_pedidos")
```

Essas métricas representam:

- **Mínimo:** menor quantidade de pedidos por cliente.
- **Máximo:** maior quantidade de pedidos por cliente.
- **Média:** quantidade média de pedidos por cliente.

---

# 21. Conclusão da análise de recorrência

O código utiliza o valor máximo para determinar se existe recorrência.

```python
if metricas['maximo'] == 1:
```

Se o máximo for `1`, nenhum cliente possui mais de um pedido.

Caso contrário, existe pelo menos um cliente com pedidos recorrentes.

Essa análise ajuda a identificar um aspecto importante do comportamento dos clientes.

---

# 22. Fluxo geral do projeto

O fluxo executado pelo notebook pode ser resumido assim:

```text
                 ┌─────────────────────┐
                 │       .env          │
                 │ Credenciais Azure   │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │ Service Principal   │
                 │ Entra ID            │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │ Azure Data Lake     │
                 │ Storage Gen2        │
                 └──────────┬──────────┘
                            │
                 ┌──────────┴──────────┐
                 ▼                     ▼
        ecommerce_pedidos.csv   ecommerce_clientes.csv
                 │                     │
                 ▼                     ▼
          Spark DataFrame       Spark DataFrame
                 │                     │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │       EDA           │
                 │ Estatísticas        │
                 │ Qualidade           │
                 │ Integridade         │
                 │ Recorrência         │
                 └─────────────────────┘
```

---

# 23. Principais conceitos utilizados

| Conceito | Aplicação |
|---|---|
| Azure Data Lake Gen2 | Armazenamento dos arquivos |
| Service Principal | Autenticação no Azure |
| OAuth | Autenticação do Spark no ADLS |
| ABFSS | Protocolo de acesso ao ADLS Gen2 |
| Spark DataFrame | Processamento dos dados |
| PySpark | Transformações e análises |
| EDA | Análise exploratória |
| Data Quality | Verificação de valores nulos |
| Data Integrity | Validação entre pedidos e clientes |
| GroupBy | Agrupamento dos dados |
| Aggregations | Cálculo de métricas |
| `datediff` | Cálculo de diferença entre datas |

---

# 24. Resultado final

O notebook implementa um fluxo completo e simples de análise:

**Autenticação → ADLS → Leitura com Spark → Exploração → Qualidade → Integridade → Análise de comportamento**

Além de demonstrar o acesso ao Azure Data Lake, o código aplica conceitos fundamentais de Engenharia de Dados e análise exploratória utilizando PySpark.

# 25. Teste de Permissão — Spark JDBC Nativo

Após as análises no Data Lake, o notebook realiza um teste final para validar a comunicação do **Databricks Serverless com um SQL Server** utilizando o conector nativo:

```python
.format("sqlserver")
```

O objetivo é verificar se o ambiente possui permissão para **gravar e ler dados** no banco SQL.

---

## 25.1 Carregamento das credenciais do SQL Server

As informações de conexão são carregadas do arquivo `.env`:

```python
SQL_HOST     = os.getenv("jdbc_hostname")
SQL_DATABASE = os.getenv("jdbc_database")
SQL_USERNAME = os.getenv("jdbc_username")
SQL_PASSWORD = os.getenv("jdbc_password")
```

As variáveis representam:

- `jdbc_hostname`: endereço do SQL Server.
- `jdbc_database`: banco de dados de destino.
- `jdbc_username`: usuário de acesso.
- `jdbc_password`: senha.

O código verifica se todas as variáveis foram carregadas antes de continuar.

---

## 25.2 Criação de dados fictícios

É criado um pequeno DataFrame apenas para testar a escrita:

```python
dados_teste = [("Databricks Serverless", "Sucesso")]
colunas = ["origem", "status"]

df_teste = spark.createDataFrame(
    dados_teste,
    colunas
)
```

---

## 25.3 Escrita no SQL Server

A tabela de teste é definida:

```python
tabela_teste = "gold_teste_permissao_spark"
```

O Spark utiliza o conector nativo:

```python
df_teste.write \
    .format("sqlserver")
```

As opções informam o servidor, porta, banco, usuário, senha e tabela.

O modo:

```python
.mode("overwrite")
```

permite substituir a tabela de teste caso ela já exista.

### Objetivo do teste

Confirmar:

- Conectividade com o SQL Server.
- Autenticação válida.
- Permissão para criar/escrever a tabela.
- Permissão para inserir os dados.

---

## 25.4 Leitura da tabela criada

Depois da escrita, o código realiza uma nova operação utilizando:

```python
spark.read \
    .format("sqlserver")
```

A mesma tabela é consultada e o resultado é armazenado em:

```python
df_validacao
```

Depois, os dados são exibidos com:

```python
display(df_validacao)
```

Essa etapa confirma que os dados gravados podem ser recuperados pelo Spark.

---

## 25.5 Validação completa

O teste representa o seguinte fluxo:

```text
Databricks Serverless
        │
        │ Spark
        ▼
   SQL Server
        │
        ├── WRITE
        │     ▼
        │  gold_teste_permissao_spark
        │
        └── READ
              ▼
        df_validacao
```

Portanto, é validado o fluxo:

**Databricks → SQL Server → Databricks**

---

## 25.6 Tratamento de erros

Toda a operação está dentro de:

```python
try:
    ...
except Exception as e:
    ...
```

Se ocorrer algum problema de conexão, autenticação ou permissão, o erro é capturado e exibido.

Isso facilita a identificação de problemas relacionados ao conector ou às credenciais.

---

# 26. Fluxo completo atualizado do projeto

Com essa etapa final, o notebook representa:

```text
                  ┌─────────────────────┐
                  │       .env          │
                  │ Credenciais Azure   │
                  │ + SQL Server        │
                  └──────────┬──────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
      Service Principal                SQL Credentials
              │                             │
              ▼                             ▼
       Azure Data Lake                 SQL Server
              │                             ▲
              ▼                             │
       Arquivos CSV                       │
              │                             │
              ▼                             │
       Spark DataFrames                    │
              │                             │
              ▼                             │
       EDA + Data Quality                  │
              │                             │
              ▼                             │
       Data Integrity                      │
              │                             │
              ▼                             │
       Análise de Clientes                 │
              │                             │
              └──────────────┬──────────────┘
                             ▼
                   Teste Spark SQL Server
                             │
                       WRITE + READ
                             │
                             ▼
                  gold_teste_permissao_spark
```

---

# 27. Resultado esperado

Se a execução terminar com sucesso, o notebook terá validado:

1. **Leitura dos dados do Azure Data Lake Gen2 usando Spark.**
2. **Escrita e leitura de dados no SQL Server utilizando o conector nativo do Spark.**

O fluxo geral pode ser resumido como:

```text
Azure Data Lake
      ↓
    Spark
      ↓
Transformações / EDA
      ↓
  SQL Server
```

---

# Ingestão Bronze – Squad 3 (Itens de Venda e Vendas)

Notebook Databricks que lê os CSVs históricos da camada **Raw** no ADLS Gen2, adiciona colunas de auditoria e grava em **Delta** na camada **Bronze**, particionado por **ano e mês** de `dt_venda`. A ingestão é **incremental**: só grava quando o arquivo de origem foi modificado e apenas registros ainda inexistentes na Bronze.

> **Frequência:** executado **uma vez por semana, toda segunda-feira** (via Databricks Job – ver seção [Agendamento](#agendamento-semanal)).

---

## Funcionalidades

- Autenticação no ADLS Gen2 via **Service Principal (OAuth)**, passada como opções (`ADLS_OPTIONS`) em cada leitura e escrita, pois o ambiente (Databricks Serverless) não permite `spark.conf.set` para `fs.azure.*`.
- Credenciais lidas de arquivo `.env` (com fallback para **Databricks Secrets**).
- Leitura dos CSVs históricos (`physical_itens_venda_caixa.csv` e `physical_vendas_caixa.csv`).
- Colunas de auditoria:
  - `bronze_ingested_at`: timestamp da ingestão.
  - `bronze_source_file`: caminho real do arquivo de origem (`_metadata.file_path`).
- Verificação de duplicidade de `id_transacao` em vendas, incluindo datas divergentes.
- Enriquecimento dos itens com `dt_venda` (via `LEFT JOIN`), sem perder registros.
- **Ingestão incremental e idempotente:**
  - compara a data de modificação do CSV com a última ingestão da Bronze;
  - se o arquivo não mudou, não grava nada;
  - se mudou, grava só os registros cuja chave ainda não existe na Bronze.
- Gravação em **Delta**, modo **`append`**, particionada por `ano_venda` e `mes_venda`.
- Validação pós-gravação (contagem por partição e registros sem data).

---

## Pré-requisitos

| Item | Detalhe |
|---|---|
| Runtime | Databricks Serverless (ou Runtime 11.3+, necessário para `_metadata.file_path`) |
| Biblioteca | `python-dotenv` (instalada na 1ª célula) |
| Acesso | Service Principal com permissão de leitura na Raw e escrita na Bronze |
| Arquivo `.env` | No diretório do notebook no Workspace |

Variáveis esperadas:

```env
CLIENT_ID=...
CLIENT_SECRET=...
TENANT_ID=...
STORAGE_ACCOUNT=...
CONTAINER=...
```

Alternativa recomendada: criar as mesmas chaves em um Secret Scope (`squad3`, configurável em `SECRET_SCOPE`). O notebook tenta o `.env` primeiro e depois os Secrets.

---

## Estrutura de dados

**Entrada (Raw)**

```
abfss://<container>@<storage>.dfs.core.windows.net/batch-data/physical_itens_venda_caixa.csv
abfss://<container>@<storage>.dfs.core.windows.net/batch-data/physical_vendas_caixa.csv
```

**Saída (Bronze, Delta)**

```
abfss://<container>@<storage>.dfs.core.windows.net/bronze/squad3/itens_venda
abfss://<container>@<storage>.dfs.core.windows.net/bronze/squad3/vendas
```

Colunas adicionadas:

| Coluna | Tipo | Descrição |
|---|---|---|
| `bronze_ingested_at` | timestamp | Momento da ingestão |
| `bronze_source_file` | string | Arquivo de origem |
| `ano_venda` | int | Ano de `dt_venda` (partição) |
| `mes_venda` | int | Mês de `dt_venda` (partição) |

Registros com `dt_venda` nulo ou sem venda correspondente vão para a partição explícita `ano_venda=0 / mes_venda=0`, em vez de `__HIVE_DEFAULT_PARTITION__`.

---

## Passo a passo do notebook

| # | Célula | O que faz |
|---|---|---|
| 1 | Instalação | `pip install python-dotenv` e `restartPython()` |
| 2 | Configuração | Carrega credenciais (`.env` → Secrets), valida, monta o endpoint OAuth e os caminhos Raw/Bronze |
| 3 | Funções auxiliares | `ADLS_OPTIONS` (credenciais OAuth); `ler_csv_adls()` (lê o CSV e captura `bronze_source_file`); `ler_delta()`; `delta_existe()`; `arquivo_modificado()` (decide se há o que ingerir); `somente_novos()` (filtra registros já existentes na Bronze) |
| 4 | Leitura e inspeção | Carrega `df_itens_venda` e `df_vendas` da Raw e exibe o schema de cada um |
| 5 | Qualidade | Detecta `id_transacao` duplicado em vendas e avisa se há datas divergentes |
| 6 | Lookup de datas | Gera `df_vendas_data` (`id_transacao`, `dt_venda`) com uma linha por transação (`min(dt_venda)`) |
| 7 | Join | `LEFT JOIN` de itens com o lookup, sem perder itens órfãos nem replicar linhas |
| 8 | Auditoria e partição | Adiciona `bronze_ingested_at`, `ano_venda`, `mes_venda` (nulos → 0). Remove `dt_venda` dos itens para manter a estrutura original |
| 9 | Gravação incremental | Para cada tabela: verifica se o CSV foi modificado; se sim, grava em Delta (`append`, `partitionBy("ano_venda","mes_venda")`) apenas os registros novos |
| 10 | Validação | Lê a Bronze e mostra total, contagem por partição e registros em `ano_venda=0` (com percentual e aviso acima de 5%) |

### Lógica incremental (células 3 e 9)

1. **Existe Bronze?** `delta_existe()` verifica o diretório `_delta_log` do destino com leitura de arquivos (`binaryFile`) usando `ADLS_OPTIONS`. A análise do plano é forçada (`.columns`), pois no Serverless (Spark Connect) a leitura é *lazy* e o erro só apareceria depois. Só o erro de "caminho inexistente" indica primeira carga, quando tudo é gravado. Qualquer outro erro (autenticação, rede, permissão) **interrompe** a execução.
2. **O arquivo mudou?** Compara o `modificationTime` do CSV (lido com `binaryFile`) com o maior `bronze_ingested_at` da Bronze, ambos em *epoch* (segundos), evitando problemas de fuso horário.
3. **O que gravar?** Se mudou, aplica um `left_anti` pela chave da tabela e grava só o que ainda não existe:

| Tabela | Chave |
|---|---|
| `itens_venda` | `id_item_venda` |
| `vendas` | `id_transacao` |

4. **Nada mudou?** A tabela é ignorada e a Bronze permanece como está.

---

## Decisões de projeto

- **`append`:** requisito do projeto, mantido nas duas gravações.
- **Incremental em vez de reprocessar tudo:** como o notebook roda toda semana sobre o mesmo CSV histórico, a checagem de modificação e o `left_anti` evitam duplicar a Bronze.
- **Erro não vira primeira carga:** a existência da Bronze é testada explicitamente, sem `except Exception` genérico.
- **Autenticação por opções, não por sessão:** o ambiente Serverless rejeita `spark.conf.set` para `fs.azure.*`, então `ADLS_OPTIONS` é aplicado em cada `read`/`write`. Por isso `DeltaTable.isDeltaTable` (incompatível com Serverless) e `dbutils.fs.ls` não são usados (dependem da configuração de sessão), e a existência da Bronze é verificada listando o `_delta_log` com `ADLS_OPTIONS`.
- **`LEFT JOIN`:** a Bronze não deve perder dados; itens sem venda são mantidos.
- **Deduplicação só no lookup:** evita multiplicar itens no join, sem alterar o dado cru de vendas.
- **`min(dt_venda)`:** escolha determinística quando há duplicatas com datas diferentes.
- **`_metadata.file_path`:** capturado na leitura (antes do join), funciona com Unity Catalog.

---

## Agendamento semanal

O notebook não se agenda sozinho. Configure um **Databricks Job** com:

- **Cron (Quartz):** `0 0 6 ? * MON` (toda segunda-feira, 06:00)
- **Fuso:** `America/Sao_Paulo`

Trecho equivalente em JSON:

```json
"schedule": {
  "quartz_cron_expression": "0 0 6 ? * MON",
  "timezone_id": "America/Sao_Paulo",
  "pause_status": "UNPAUSED"
}
```

---

## Observações e limitações

- **Registros alterados não são atualizados:** o `append` com `left_anti` só insere chaves novas. Se um registro já ingerido mudar de valor no CSV, a Bronze mantém a versão antiga.
- **Chave nula:** itens com `id_item_venda` nulo nunca "casam" no `left_anti` e seriam inseridos novamente a cada leitura de arquivo modificado. Esses registros são tratados na Silver (`PK_NULA`).
- **Duplicatas no próprio CSV:** a Bronze preserva o dado cru. Se o arquivo trouxer a mesma chave mais de uma vez (ex.: `id_transacao` em vendas), todas as ocorrências de uma chave nova são gravadas; a deduplicação é feita na Silver.
- **Venda tardia:** um item ingerido antes de sua venda existir fica na partição `0/0`, e não é corrigido depois.
- **Sem registros novos:** quando o arquivo mudou mas não há chaves novas, nada é gravado e `bronze_ingested_at` não avança. Na semana seguinte o arquivo ainda aparece como "modificado", o que só custa uma releitura.
- **Reupload do arquivo:** qualquer `touch` ou reenvio do CSV dispara a leitura; a duplicação é evitada pelo `left_anti`.
- Se `dt_venda` for inferida como `string` em formato não padrão (ex.: `dd/MM/yyyy`), `year()`/`month()` retornarão nulo e tudo cairá em `ano_venda=0`. A célula 10 evidencia isso; nesse caso, converta com `to_date(col, "dd/MM/yyyy")`.
- **Ordem das células:** as funções auxiliares (`arquivo_modificado`, `somente_novos`, `delta_existe`, `ler_delta`) devem ser definidas **somente na célula 3**. Redefini-las em outra célula (por exemplo, com `DeltaTable.isDeltaTable`) sobrescreve as versões compatíveis e causa o erro `AZURE_INVALID_CREDENTIALS_CONFIGURATION`.
- Se o ambiente passar a permitir configuração de sessão, ou o workspace usar External Location/Unity Catalog, `ADLS_OPTIONS` deixa de ser necessário.

---

# Camada Silver – Itens de Venda (Squad 3)

Notebook `05_silver_treat`. **Lê a Bronze (Delta)**, aplica regras de qualidade e enriquecimentos, adiciona `silver_processed_at` e grava em **Delta Silver particionado por ano e mês**.

É um notebook **independente** do de ingestão Bronze (ver `README.md`): não herda variáveis nem funções dele, então tem as próprias células de configuração (1 a 3). Deve rodar **depois** da Bronze, como uma tarefa dependente no mesmo Job semanal.

> **Bronze × Silver:** a Bronze guarda o dado cru, apenas com auditoria (`bronze_ingested_at`, `bronze_source_file`) e partição. Todo enriquecimento de negócio (`id_loja`, `tipo_produto`, `venda_em_feriado`, validações) é feito aqui.

---

## Funcionalidades

- Leitura da Bronze de itens e de vendas (Delta), sem tocar nos CSVs originais, exceto no cadastro de perecíveis.
- **Processamento incremental**: itens cujo `id_item_venda` já está na Silver não são reprocessados (compatível com `mode("append")`). Se a Bronze não recebeu dados novos, nada é adicionado à Silver.
- Regras de qualidade **aplicadas**: registros inválidos vão para uma tabela de **quarentena** com o motivo da rejeição.
- Enriquecimento com `id_loja` e `dt_venda` (da Bronze de vendas).
- Classificação do produto em `PERECIVEL` ou `SECO` a partir do cadastro de perecíveis.
- Flag `venda_em_feriado` (feriados nacionais, incluindo Sexta-feira Santa).
- Coluna de auditoria `silver_processed_at`.
- Gravação em Delta, `append`, particionada por `ano_venda` e `mes_venda`.
- Validações antes da gravação (interrompem o processo se a Silver violar uma regra) e leitura de conferência depois.

---

## Pré-requisitos

| Item | Detalhe |
|---|---|
| Bronze populada | `bronze/squad3/itens_venda` e `bronze/squad3/vendas` (Delta). Sem elas, o notebook falha de propósito |
| Ambiente | Databricks Serverless |
| Biblioteca | `python-dotenv` (instalada na 1ª célula) |
| Arquivo `.env` | Mesmas variáveis do notebook da Bronze: `CLIENT_ID`, `CLIENT_SECRET`, `TENANT_ID`, `STORAGE_ACCOUNT`, `CONTAINER` (ou Secret Scope `squad3`) |
| Cadastro | `batch-data/physical_produtos_pereciveis.csv` na Raw |

---

## Entradas e saídas

| Tipo | Caminho |
|---|---|
| Entrada | `.../bronze/squad3/itens_venda` (Delta) |
| Entrada | `.../bronze/squad3/vendas` (Delta) |
| Entrada | `.../batch-data/physical_produtos_pereciveis.csv` (Raw) |
| Saída | `.../silver/squad3/itens_venda` (Delta, partição `ano_venda`/`mes_venda`) |
| Saída | `.../silver/squad3/itens_venda_rejeitados` (Delta, quarentena) |

### Esquema da Silver (`itens_venda`)

| Coluna | Descrição |
|---|---|
| `id_item_venda` | PK do item |
| `id_transacao` | FK para a venda |
| `id_loja` | Loja (vem da Bronze de vendas) |
| `codigo_barras_produto` | Código do produto |
| `quantidade` | `DECIMAL(18,3)` |
| `preco_unitario_registro` | `DECIMAL(10,2)` |
| `valor_total_item` | `DECIMAL(10,2)` |
| `dt_venda` | Data da venda |
| `unidade_medida_produto` | `kg`, `l` ou `un` |
| `tipo_produto` | `PERECIVEL` ou `SECO` |
| `venda_em_feriado` | `true` se a venda ocorreu em feriado nacional |
| `silver_processed_at` | Timestamp do processamento Silver |
| `ano_venda`, `mes_venda` | Partições (nulos → `0`) |

### Esquema da quarentena (`itens_venda_rejeitados`)

`id_item_venda`, `id_transacao`, `codigo_barras_produto`, `quantidade`, `preco_unitario_registro`, `valor_total_item`, `bronze_source_file` (todos como `string`), `motivo_rejeicao` e `quarentena_at`.

---

## Passo a passo (células)

| # | Célula | O que faz |
|---|---|---|
| 1 | Instalação | `%pip install python-dotenv`. Deve ser a **primeira** célula; o `restartPython()` fica comentado, pois o `%pip` já reinicia o estado quando instala algo novo. Não a reexecute no meio do notebook, pois isso pode limpar as variáveis |
| 2 | Configuração | Carrega credenciais (`.env` → Secrets), valida, monta o endpoint OAuth e os caminhos Raw/Bronze |
| 3 | Funções auxiliares | `ADLS_OPTIONS` (OAuth), `ler_csv_adls()`, `ler_delta()` e `delta_existe()` |
| 4 | Caminhos | Define caminhos da Silver, da quarentena e do CSV de perecíveis |
| 5 | Leitura Bronze | Verifica se a Bronze existe (se não existir, **interrompe com erro**, sem recriá-la a partir da Raw), lê itens e vendas em Delta e mostra contagem e amostra |
| 6 | Referência de vendas | Uma linha por `id_transacao` (`min(dt_venda)`, `min(id_loja)`), de forma determinística |
| 7 | Perecíveis (Raw) | Lê o cadastro de produtos perecíveis |
| 8 | Referência de perecíveis | `sku`, unidade normalizada (minúscula) e `eh_perecivel`, sem duplicar SKU |
| 9 | Quarentena (helper) | Função que padroniza os registros rejeitados com `motivo_rejeicao` |
| 10 | PK e incremental | Separa PK nula, remove duplicados (mantém o mais recente por `bronze_ingested_at`) e exclui itens já presentes na Silver |
| 11 | FK e enriquecimento | Separa itens órfãos (`left_anti`) e faz o `inner join` com vendas para trazer `id_loja` e `dt_venda` |
| 12 | Classificação | `left join` com perecíveis define `tipo_produto`; SECO recebe unidade `un` |
| 13 | Quantidade | Cast `DECIMAL(18,3)` e `quantidade_valida` (> 0; kg/l decimal; un inteiro) |
| 14 | Preço | Cast `DECIMAL(10,2)` e `preco_valido` (> 0) |
| 15 | Valor total | Cast, `valor_total_calculado` e `valor_total_consistente` (null-safe) |
| 16 | Feriados | Gera a tabela de feriados nacionais por ano |
| 17 | Flag de feriado | Join por data e cria `venda_em_feriado` |
| 18 | Auditoria | Adiciona `silver_processed_at` |
| 19 | Aplicação das regras | Gera `motivo_rejeicao`; válidos seguem para a Silver, inválidos vão para a quarentena |
| 20 | Estrutura final | Cria `ano_venda`/`mes_venda` e seleciona as colunas finais |
| 21 | Validação | Métricas de qualidade (registros, perecíveis, PK, FK, quantidade, preço, feriados) e quarentena por motivo; interrompe a gravação se houver violação |
| 22 | Gravação | Quarentena (`overwrite`) e Silver (`append`, particionada) |
| 23 | Conferência | Lê a Silver gravada e mostra schema, amostra e contagem por partição |

---

## Regras de negócio e como são aplicadas

| Regra | Aplicação |
|---|---|
| `id_item_venda` não nulo nem duplicado | PK nula → quarentena (`PK_NULA`); duplicados na Bronze reduzidos a 1 (mais recente); itens já na Silver são excluídos antes da gravação; a validação (célula 21) interrompe se restar nulo ou duplicado |
| `id_transacao` existe em vendas (FK) | Órfãos → quarentena (`FK_ORFA`) |
| `quantidade` > 0; kg/L decimal; un inteiro | Inválidos → quarentena (`QUANTIDADE_INVALIDA`). Produtos SECO são tratados como `un` |
| `preco_unitario_registro` > 0 e `DECIMAL(10,2)` | Inválidos (inclui nulo e estouro do cast) → quarentena (`PRECO_INVALIDO`) |
| `valor_total_item = preco × quantidade` | Diferença maior que R$ 0,01 ou nulo → quarentena (`VALOR_TOTAL_INCONSISTENTE`) |
| Flag `venda_em_feriado` | Calculada por data completa |

Um registro pode ter mais de um motivo (separados por `;`).

---

## Decisões de projeto

- **Silver construída da Bronze:** `id_loja` e `dt_venda` vêm da Bronze de vendas, pois a Bronze de itens não mantém `dt_venda` nem `id_loja`. Assim a Silver não depende da partição `0/0` que a Bronze possa ter atribuído a um item cuja venda chegou depois.
- **`append` mantido** na Silver. A unicidade da PK é garantida pelo `left_anti` contra a Silver e pela deduplicação na entrada.
- **Autenticação por opções:** toda leitura e escrita no ADLS usa `ADLS_OPTIONS` (célula 3), pois o Serverless não aceita `spark.conf.set` para `fs.azure.*`.
- **Existência da Silver:** `delta_existe()` verifica o `_delta_log` com leitura de arquivos e força a análise do plano, porque no Serverless (Spark Connect) a leitura é *lazy* e o erro de caminho inexistente só apareceria depois.
- **Quarentena em `overwrite`:** como a Silver só reprocessa itens ainda não aprovados, a quarentena é sempre o retrato atual dos rejeitados, sem acumular duplicatas a cada execução.
- **Tolerância de R$ 0,01** na consistência do valor total, para absorver diferenças de arredondamento com quantidades fracionadas.
- **Unidade SECO = `un`:** o cadastro só traz unidade dos perecíveis; itens fora dele são considerados unitários (quantidade inteira).
- **Unidades fora de kg/l/un** (por exemplo `g`, `ml`) em perecíveis passam na regra de unidade, pois não há regra definida para elas.
- **Feriados:** fixos (01/01, 21/04, 01/05, 07/09, 12/10, 02/11, 15/11, 25/12), **Sexta-feira Santa** (calculada a partir da Páscoa) e **20/11 apenas a partir de 2024** (Lei 14.759/2023). Carnaval e Corpus Christi não são feriados nacionais e ficam de fora. Os anos cobertos vão de 2000 até o ano seguinte ao atual.
- **Partição nula:** `dt_venda` nula resulta em `ano_venda=0` e `mes_venda=0`, igual à Bronze.

---

## Cobertura dos requisitos

| Requisito | Situação | Onde |
|---|---|---|
| Ler a camada Bronze | Atendido | Células 5 e 6 |
| Aplicar transformações Silver | Atendido | Células 10 a 19 |
| Adicionar `silver_processed_at` | Atendido | Célula 18 |
| Salvar como Delta Silver particionado por ano e mês | Atendido | Células 20 e 22 |
| PK não nula nem duplicada | Atendido | Células 10, 19 e 21 |
| FK de `id_transacao` em vendas | Atendido | Célula 11 |
| Regra de quantidade (kg/L float, un inteiro) | Atendido | Célula 13 |
| Preço > 0 e `DECIMAL(10,2)` | Atendido | Célula 14 |
| Consistência de `valor_total_item` | Atendido | Célula 15 |
| Flag `venda_em_feriado` | Atendido | Células 16 e 17 |
| KPIs de Batch | **Fora deste notebook** | Camada Gold (ver abaixo) |

---

## KPIs de Batch (consumo da Silver)

Os KPIs abaixo **não são calculados neste notebook**; devem ser construídos sobre a Silver (camada Gold). A Silver já contém as colunas necessárias:

| KPI | Colunas usadas |
|---|---|
| Receita por produto por loja por mês | `id_loja`, `codigo_barras_produto`, `valor_total_item`, `ano_venda`, `mes_venda` |
| Top 10 produtos por loja por trimestre (quantidade e receita) | `id_loja`, `codigo_barras_produto`, `quantidade`, `valor_total_item`, `dt_venda` |
| Crescimento MoM da receita de perecíveis vs secos por loja | `id_loja`, `tipo_produto`, `valor_total_item`, `ano_venda`, `mes_venda` |
| Quantidade média de itens por transação por loja | `id_loja`, `id_transacao`, `id_item_venda` |
| Análise de feriados | `venda_em_feriado`, `quantidade`, `dt_venda` |

Itens rejeitados não entram nos KPIs, e por isso a quarentena deve ser acompanhada.

---

## Agendamento

Configure no mesmo Job semanal (segunda-feira, `0 0 6 ? * MON`, fuso `America/Sao_Paulo`) duas tarefas encadeadas:

1. **Bronze** (notebook de ingestão).
2. **Silver** (`05_silver_treat`), com dependência da tarefa Bronze, para só iniciar quando a ingestão terminar com sucesso.

---

## Observações e pontos de atenção

- **Ordem de execução:** as células 11 a 20 formam uma cadeia (`df_silver = df_silver...`), e a 19 depende de colunas que a 20 descarta. Execute o notebook de cima para baixo (*Run all*). Se alterar uma regra, reexecute a partir da célula 11; rodar uma célula isolada ou repetida gera colunas duplicadas ou ausentes.
- **Dependência `python-dotenv`:** só é necessária para ler o `.env`. Alternativas para dispensar a célula 1: cadastrar a biblioteca no painel *Environment* do notebook/Job Serverless, ou usar apenas Databricks Secrets (a função `obter_config` já faz o fallback).
- **Semana sem alteração no CSV:** a Bronze não é modificada, a Silver não encontra itens novos e nada novo é gravado. A quarentena continua refletindo os itens ainda rejeitados, pois eles não estão na Silver e são reavaliados a cada execução.
- **Venda tardia:** um item cuja venda ainda não existe vai para a quarentena (`FK_ORFA`) e é promovido para a Silver numa execução seguinte, quando a venda chegar à Bronze.
- **Registros alterados na origem** não são atualizados na Bronze (limitação do `append` incremental) e, portanto, também não chegam atualizados à Silver.
- **Reprocessamento:** o Serverless não permite `cache()`, então cada ação (validação, gravações) recalcula o plano a partir da Bronze. Em volumes muito maiores, considere dividir o processamento por partição.
- **Tipo do código de barras:** o join com perecíveis compara `codigo_barras_produto` e `sku` como `string`. Se todos os itens saírem como `SECO`, verifique se o tipo inferido (ex.: `double`) está gerando códigos como `7.89E12`.
- **Formato de `dt_venda`:** se vier como `string` em formato não padrão (ex.: `dd/MM/yyyy`), `year()`/`month()` retornam nulo e os itens caem em `ano_venda=0`. Nesse caso, converta com `to_date(col, "dd/MM/yyyy")` na referência de vendas (célula 6).
- **Auditoria da Bronze:** `bronze_ingested_at` e `bronze_source_file` não são carregadas para a Silver; `bronze_source_file` segue na quarentena para rastrear a origem do erro.
- Acompanhe a tabela `itens_venda_rejeitados`: crescimento de `FK_ORFA` indica problema na ingestão de vendas; `VALOR_TOTAL_INCONSISTENTE` indica erro de PDV.

---
Esse padrão pode servir como base para uma arquitetura em que os dados processados no Data Lake sejam posteriormente disponibilizados em uma camada **Gold** no SQL Server.

