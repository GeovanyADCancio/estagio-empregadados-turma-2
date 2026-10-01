# Databricks notebook source
# DBTITLE 1,Ingestao Bronze
# MAGIC %md
# MAGIC # Ingestao Bronze — Controle de Estado + JDBC Seguro
# MAGIC
# MAGIC Este notebook combina a filtragem incremental por hash SHA-256 com o acesso JDBC seguro usando .env dinamico (solucao do colega). Inclui comparacao com Lakehouse Federation e funcao de escrita segura.

# COMMAND ----------

# DBTITLE 1,Resolucao dinamica do .env
# ============================================================================
# Passo 1 — Resolucao dinamica do .env (solucao do colega)
# ============================================================================

import os
from dotenv import load_dotenv
from pathlib import Path

def descobrir_env_via_contexto_notebook():
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        notebook_path = ctx.notebookPath().get()
        pasta_notebook = "/".join(notebook_path.split("/")[:-1])
        return f"/Workspace{pasta_notebook}/.env"
    except Exception:
        env_found = next(Path("/Workspace").rglob(".env"), None)
        return str(env_found) if env_found else None

dotenv_path = descobrir_env_via_contexto_notebook()

if dotenv_path and os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"✅ .env carregado de: {dotenv_path}")
else:
    print("❌ .env nao encontrado.")

sql_host = os.getenv("SQL_HOST").strip('"').strip("'")
sql_database = os.getenv("SQL_DATABASE").strip('"').strip("'")
sql_user = os.getenv("SQL_USERNAME").strip('"').strip("'")
sql_password = os.getenv("SQL_PASSWORD").strip('"').strip("'")

# COMMAND ----------

# DBTITLE 1,Funcoes JDBC + leitura
# ============================================================================
# Passo 2 — Funcoes JDBC (somente leitura) + leitura das tabelas
# ============================================================================

from pyspark.sql import functions as F

def ler_tabela_jdbc(schema_tabela):
    return (
        spark.read.format("sqlserver")
        .option("host", sql_host).option("port", 1433)
        .option("database", sql_database).option("dbtable", schema_tabela)
        .option("user", sql_user).option("password", sql_password)
        .option("encrypt", "true").option("trustServerCertificate", "false")
        .option("readOnly", "true").load()
    )

def bloquear_escrita_jdbc():
    raise PermissionError("🔒 ESCRITA BLOQUEADA! Use workspace.default.<tabela> para gravar.")

df_pedidos = spark.table("sqlserver_catalog.squad2.ecommerce_pedidos")

print(f"📊 Total de pedidos na origem: {df_pedidos.count()}")
print("🔒 JDBC configurado como SOMENTE LEITURA")
display(df_pedidos.limit(5))

# COMMAND ----------

# DBTITLE 1,Criar tabela de controle
# ============================================================================
# Passo 3 — Criar tabela de controle de estado (se nao existir)
# ============================================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.default.controle_pedidos (
        id_pedido           BIGINT,
        hash_registro       STRING,
        dt_processamento    TIMESTAMP
    )
    USING DELTA
    COMMENT 'Controle de estado: rastreia quais pedidos ja foram processados.'
""")

print("✅ Tabela de controle verificada/criada: workspace.default.controle_pedidos")

# COMMAND ----------

# DBTITLE 1,Hash e filtragem
# ============================================================================
# Passo 4 — Calcular hash e filtrar apenas novos/alterados
# ============================================================================

colunas_hash = [c for c in df_pedidos.columns if c != "id_pedido"]

expr_hash = F.sha2(
    F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in colunas_hash]),
    256
)

df_com_hash = df_pedidos.withColumn("hash_registro", expr_hash)
df_controle = spark.table("workspace.default.controle_pedidos")

df_diff = df_com_hash.alias("src").join(
    df_controle.alias("ctl"),
    F.col("src.id_pedido") == F.col("ctl.id_pedido"),
    "left"
).select(
    F.col("src.id_pedido"),
    F.col("src.hash_registro").alias("hash_novo"),
    F.col("ctl.hash_registro").alias("hash_antigo"),
)

df_novos = df_diff.filter(
    F.col("hash_antigo").isNull() | (F.col("hash_novo") != F.col("hash_antigo"))
)

total_novos = df_novos.count()
print(f"🆕 Registros novos/alterados a processar: {total_novos}")

if total_novos > 0:
    df_pedidos_novos = df_com_hash.join(
        df_novos.select("id_pedido"), "id_pedido", "inner"
    ).drop("hash_registro")
    display(df_novos.limit(10))
else:
    print("✅ Nenhum registro novo — nada a processar.")

# COMMAND ----------

# DBTITLE 1,Salvar e atualizar controle
# ============================================================================
# Passo 5 — Salvar registros e atualizar controle de estado
# ============================================================================

if total_novos > 0:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS workspace.default.pedidos_filtrados (
            id_pedido                   BIGINT,
            id_cliente                  BIGINT,
            id_endereco_entrega         BIGINT,
            dt_pedido                   STRING,
            status_pedido               STRING,
            valor_total                 DOUBLE,
            valor_frete                 DOUBLE,
            metodo_pagamento            STRING,
            dt_previsao_entrega         STRING,
            dt_ultima_atualizacao_status STRING
        )
        USING DELTA
    """)
    
    df_pedidos_novos.write.mode("append").saveAsTable("workspace.default.pedidos_filtrados")
    print(f"✅ {total_novos} registros inseridos em pedidos_filtrados")
    
    from delta.tables import DeltaTable
    delta_controle = DeltaTable.forName(spark, "workspace.default.controle_pedidos")
    df_atualizacao = df_novos.select(
        F.col("id_pedido"),
        F.col("hash_novo").alias("hash_registro"),
        F.current_timestamp().alias("dt_processamento")
    )
    
    delta_controle.alias("ctl").merge(
        df_atualizacao.alias("src"), "ctl.id_pedido = src.id_pedido"
    ).whenMatchedUpdate(set={
        "hash_registro": "src.hash_registro",
        "dt_processamento": "src.dt_processamento"
    }).whenNotMatchedInsert(values={
        "id_pedido": "src.id_pedido",
        "hash_registro": "src.hash_registro",
        "dt_processamento": "src.dt_processamento"
    }).execute()
    
    print(f"✅ Controle atualizado: {total_novos} hashes registrados.")
else:
    print("⏭️ Nada a salvar — controle ja esta em dia.")

total_controle = spark.table("workspace.default.controle_pedidos").count()
print(f"\n📊 Resumo: Origem={df_pedidos.count()} | Novos={total_novos} | Controle={total_controle}")

# COMMAND ----------

# DBTITLE 1,Comparacao JDBC vs Lakehouse
# ============================================================================
# Passo 6 — Comparacao: JDBC vs Lakehouse Federation
# ============================================================================

print("📊 Comparacao: JDBC vs Lakehouse Federation\n")

tabelas = [
    ("squad2.ecommerce_produtos", "sqlserver_catalog.squad2.ecommerce_produtos"),
    ("dbo.ecommerce_categorias", "sqlserver_catalog.dbo.ecommerce_categorias"),
    ("dbo.ecommerce_clientes", "sqlserver_catalog.dbo.ecommerce_clientes"),
    ("squad2.ecommerce_pedidos", "sqlserver_catalog.squad2.ecommerce_pedidos"),
]

print(f"{'Tabela':<35} {'JDBC':>10} {'Lakehouse':>10} {'Match':>8}")
print("-" * 65)

for sql_table, lh_table in tabelas:
    count_jdbc = ler_tabela_jdbc(sql_table).count()
    count_lh = spark.table(lh_table).count()
    match = "✅" if count_jdbc == count_lh else "❌"
    print(f"{sql_table:<35} {count_jdbc:>10,} {count_lh:>10,} {match:>8}")

# COMMAND ----------

# DBTITLE 1,Funcao escrever_tabela_jdbc
# ============================================================================
# Passo 7 — Funcao de escrita segura via JDBC
# ============================================================================

def escrever_tabela_jdbc(df, schema_tabela, modo="overwrite"):
    total = df.count()
    if total == 0:
        raise ValueError(
            f"❌ DataFrame VAZIO! Recusando escrever em {schema_tabela}.\n"
            f"   Escrita com DataFrame vazio + mode('overwrite') trunca a tabela."
        )
    print(f"📝 Escrevendo {total} linhas em {schema_tabela} (mode={modo})...")
    (df.write.format("sqlserver").option("host", sql_host).option("port", 1433)
        .option("database", sql_database).option("dbtable", schema_tabela)
        .option("user", sql_user).option("password", sql_password)
        .option("encrypt", "true").option("trustServerCertificate", "false")
        .mode(modo).save())
    count_readback = ler_tabela_jdbc(schema_tabela).count()
    if count_readback == total:
        print(f"✅ Escrita confirmada: {count_readback} linhas")
    else:
        print(f"⚠️ ATENÇÃO: Escrito {total} mas leu {count_readback} de volta!")
    return count_readback

print("✅ Funcao escrever_tabela_jdbc() definida com 3 camadas de segurança:")
print("   1. Verifica count > 0 antes de escrever")
print("   2. Escreve via JDBC")
print("   3. Le de volta para confirmar")