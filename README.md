
PIPELINE DE DADOS: INGESTÃO REAL-TIME, QUALIDADE E CARGA RELACIONAL

Este projeto implementa um pipeline completo de Engenharia de Dados (End-to-End)
desenvolvido para o estágio EmpregaDados. A solução automatiza a captura contínua
e incremental de arquivos Parquet hospedados no Azure Data Lake Storage Gen2 (ADLS),
persiste os dados em uma tabela Delta Lake (camada Raw), executa validações de
Qualidade de Dados (Data Profiling) e carrega a base final em um banco relacional
Azure SQL Database via JDBC.

--------------------------------------------------------------------------------
1. ARQUITETURA E ESTRUTURA DOS NOTEBOOKS
--------------------------------------------------------------------------------

O pipeline adota o princípio de Separação de Responsabilidades (SoC), organizado
em 3 notebooks sequenciais:

```text
├── 01_ingestao_adls_para_delta.ipynb       # Ingestão contínua, checkpoint e Delta Raw
├── 02_qualidade_e_analise_exploratoria.sql # Data Profiling, integridade e consistência
├── 03_carga_sqlserver_jdbc.ipynb           # Exportação relacional via JDBC e validação
├── .env                                    # Variáveis de ambiente (ignorado no Git)
└── README.md                               # Documentação do projeto
```
--------------------------------------------------------------------------------
2. CONFIGURAÇÃO DO AMBIENTE E VARIÁVEIS DE AMBIENTE (.env)
--------------------------------------------------------------------------------

Crie um arquivo .env na raiz do projeto com as credenciais necessárias:

Credenciais do Azure AD / Service Principal para o ADLS Gen2
ADLS_CLIENT_ID="seu-client-id"
ADLS_TENANT_ID="seu-tenant-id"
ADLS_CLIENT_SECRET="seu-client-secret"

Credenciais de Acesso ao Azure SQL Server
SQL_HOST="seu-servidor.database.windows.net"
SQL_DATABASE="seu-banco-de-dados"
SQL_USERNAME="seu-usuario"
SQL_PASSWORD="sua-senha"

Aviso de Segurança: Certifique-se de que o .env esteja listado no .gitignore para
não expor credenciais no repositório remoto.

--------------------------------------------------------------------------------
3. DOCUMENTAÇÃO LINHA POR LINHA DOS NOTEBOOKS
--------------------------------------------------------------------------------

[NOTEBOOK 01: 01_ingestao_adls_para_delta]
Responsável por monitorar recursivamente diretórios do Data Lake, evitar 
reprocessamento de arquivos (idempotência), converter bytes em memória para 
PySpark e gravar na tabela Delta.

* Dependências e Autenticação:
  - import os, io, time, pandas as pd: Manipulação de variáveis de ambiente,
    buffers em memória RAM (io.BytesIO), controle de tempo de polling (time.sleep)
    e decodificação de Parquet.
  - from dotenv import load_dotenv: Carrega credenciais do arquivo .env.
  - from azure.identity import ClientSecretCredential: Autenticação não interativa
    via Service Principal no Azure Entra ID.
  - from azure.storage.filedatalake import DataLakeServiceClient: Cliente do SDK
    especializado no sistema de arquivos hierárquico do ADLS Gen2.
  - from pyspark.sql.functions import current_timestamp, lit / StringType:
    Criação de metadados técnicos de auditoria e conversão forçada de tipos.

* Conexão ao Storage e Validação:
  - load_dotenv(".env"): Injeta as variáveis na sessão de execução.
  - client_id, tenant_id, client_secret = ...: Recupera as chaves de acesso.
  - assert client_id and tenant_id and client_secret, ...: Validação preventiva.
  - DataLakeServiceClient(...): Inicializa o endpoint .dfs.core.windows.net.
  - file_system_client = service_client.get_file_system_client(...): Aponta para
    o container de entrada (raw).

* Controle de Checkpoint e Idempotência:
  - control_checkpoint = set(): Conjunto O(1) para armazenar arquivos lidos.
  - if spark.catalog.tableExists(delta_target_table): Consulta o metastore do Spark.
  - spark.table(...).select("source_file").distinct()...: Coleta o histórico para
    evitar duplicidade após reinicializações.

* Loop de Monitoramento Contínuo (Polling):
  - while True:: Varredura contínua.
  - file_system_client.get_paths(..., recursive=True): Busca recursiva de diretórios.
  - new_files = [...]: Filtra apenas arquivos 'ecommerce_enderecos.parquet' não lidos.
  - downloaded_bytes = file_client.download_file().readall(): Download na memória RAM.
  - pdf = pd.read_parquet(io.BytesIO(downloaded_bytes)): Conversão Bytes -> Pandas.
  - df_spark = spark.createDataFrame(pdf): Conversão Pandas -> Spark DataFrame.
  - df_spark.withColumn("complemento", cast(StringType())): Previne quebra de schema.
  - withColumn("source_file", lit(...)).withColumn("ingestion_time", current_timestamp()):
    Linhagem de dados e timestamp de auditoria.
  - df_spark.write.format("delta").mode("append").saveAsTable(...): Escrita incremental ACID.
  - control_checkpoint.add(file_path): Registra arquivo como processado.
  - time.sleep(15): Intervalo preventivo contra sobrecarga de API.
  - except KeyboardInterrupt / Exception: Interrupção limpa e tolerância a falhas.


[NOTEBOOK 02: 02_qualidade_e_analise_exploratoria]
Validações estruturais e de negócio aplicadas à tabela raw_ecommerce_enderecos:

* Amostragem (LIMIT 100): Inspeção visual das linhas e estrutura das colunas.
* Matriz de Completude (Nulos): SUM(CASE WHEN campo IS NULL THEN 1 ELSE 0 END)
  para avaliar preenchimento e ausência de coordenadas (latitude/longitude).
* Teste de Chave Primária (Unicidade):
  SELECT id_endereco, COUNT(*) ... GROUP BY id_endereco HAVING COUNT(*) > 1;
* Validação de Regra de Negócio (CEP):
  LENGTH(REGEXP_REPLACE(cep, '[^0-9]', '')) para validar o padrão de 8 dígitos.
* Linhagem e Lotes:
  Agrupamento por source_file com MIN(ingestion_time) e contagem de registros.


[NOTEBOOK 03: 03_carga_sqlserver_jdbc]
Responsável por enviar os dados consolidados para o banco relacional de serviço.

* Parâmetros de Conexão e Criptografia:
  - load_dotenv(".env"): Carrega credenciais do banco.
  - jdbc_url: String configurada com port=1433, encrypt=true,
    trustServerCertificate=false e validação de certificado hostNameInCertificate.
  - target_table = "squad1.ecommerce_enderecos_luiz_riuler": Tabela e schema de destino.

* Limpeza e Escrita:
  - df_enderecos = spark.table("raw_ecommerce_enderecos"): Leitura do Delta Lake.
  - df_enderecos.drop("source_file", "ingestion_time"): Higienização de metadados.
  - df_enderecos.write.format("sqlserver").mode("overwrite").save(): Escrita JDBC.

* Sanity Check Pós-Carga:
  - spark.read.format("jdbc")...load(): Leitura direta da tabela no Azure SQL.
  - df_validacao.count(): Validação da integridade de contagem entre origem e destino.

--------------------------------------------------------------------------------
4. COMO EXECUTAR
--------------------------------------------------------------------------------

1. Preencha as credenciais no arquivo .env.
2. Execute o notebook 01_ingestao_adls_para_delta para capturar os arquivos do
   Data Lake e popular a tabela Delta.
3. Execute o notebook 02_qualidade_e_analise_exploratoria para atestar a
   conformidade dos dados.
4. Execute o notebook 03_carga_sqlserver_jdbc para carregar e validar a tabela
   no Azure SQL Server.
================================================================================
