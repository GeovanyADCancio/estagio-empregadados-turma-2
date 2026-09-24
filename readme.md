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

Esse padrão pode servir como base para uma arquitetura em que os dados processados no Data Lake sejam posteriormente disponibilizados em uma camada **Gold** no SQL Server.