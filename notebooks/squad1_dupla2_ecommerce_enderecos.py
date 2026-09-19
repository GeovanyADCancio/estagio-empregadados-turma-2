# Databricks notebook source
# MAGIC %md
# MAGIC # Squad 1 - Data Quality | Dupla 2
# MAGIC Tabela: `ecommerce_enderecos`
# MAGIC
# MAGIC Fluxo: conectar no ADLS Gen2 -> ler/explorar -> gravar em `squad1.ecommerce_enderecos` no SQL Server.

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

import os
from dotenv import load_dotenv

# Requer "Files in Repos" habilitado (padrao em runtimes recentes): o .env fica na raiz
# do repo, um nivel acima da pasta /notebooks
load_dotenv("../.env")

client_id = os.getenv("ADLS_CLIENT_ID")
tenant_id = os.getenv("ADLS_TENANT_ID")
client_secret = os.getenv("ADLS_CLIENT_SECRET")
storage_account_name = os.getenv("ADLS_STORAGE_ACCOUNT_NAME")
container_name = os.getenv("ADLS_CONTAINER_NAME", "raw")

assert all([client_id, tenant_id, client_secret, storage_account_name]), "Faltou preencher alguma variavel no .env"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autenticacao no ADLS Gen2 (Service Principal / OAuth)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)

base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
real_time_ref_path = f"{base_path}/real-time-data"

print(f"Container: {container_name}")
print(f"Caminho de referencia (real-time): {real_time_ref_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explorar a estrutura antes de ler (confirmar pasta/formato de `ecommerce_enderecos`)

# COMMAND ----------

display(dbutils.fs.ls(real_time_ref_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Ajuste `table_path` e o `format` abaixo depois de ver a listagem acima
# MAGIC (ex: pode ser uma subpasta `ecommerce_enderecos/` com arquivos `.json` ou `.parquet`).

# COMMAND ----------

table_path = f"{real_time_ref_path}/ecommerce_enderecos"

df = (
    spark.read
    .format("json")  # trocar para "parquet" ou "csv" se for o caso, conforme a listagem acima
    .option("multiLine", "true")
    .load(table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analise exploratoria inicial

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

print(f"Linhas: {df.count()}")
print(f"Colunas: {len(df.columns)}")

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# Contagem de nulos por coluna
from pyspark.sql.functions import col, sum as spark_sum

df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).display()
