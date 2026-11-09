# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 · Engenharia de Features (Prophet) — Squad 3 | Henrique Ficht
# MAGIC
# MAGIC **Problema P1 · Modelo A (aditivo/sazonal)** — comparação com Vinicius (Modelo B · XGBoost global)
# MAGIC
# MAGIC ## Objetivo
# MAGIC Preparar os dados para o modelo. Criar colunas numéricas que ajudem a identificar padrões
# MAGIC e deixar tudo pronto pro notebook `10_prophet_treino.py` consumir.
# MAGIC
# MAGIC ## Decisões herdadas da EDA (notebook 08)
# MAGIC
# MAGIC | Decisão | Valor |
# MAGIC |---|---|
# MAGIC | Categorias no modelo | 6 (todas as perecíveis elegíveis) |
# MAGIC | Período de histórico | 2024-01 a 2026-08 (32 meses) |
# MAGIC | Split treino | 2024-01 → 2026-05 (29 meses) |
# MAGIC | Split teste | 2026-06 → 2026-08 (3 meses) |
# MAGIC | Forecast produtivo | 2026-09 → 2026-11 (3 meses) |
# MAGIC | `growth` | `'linear'` (trend plana) |
# MAGIC | `yearly_seasonality` | `True` (padrão anual claro) |
# MAGIC | `holidays` | Nenhum (zero outliers detectados) |
# MAGIC | `changepoint_prior_scale` | default (0.05) |
# MAGIC
# MAGIC ## Regressores decididos (seção 1.4 da EDA)
# MAGIC
# MAGIC Lag0 é **data leakage** pro forecast futuro (não teríamos produção do mês que queremos prever).
# MAGIC Lag1 é o único viável — produção do mês anterior é conhecida no momento do forecast.
# MAGIC
# MAGIC | Categoria | Correlação lag1 | Decisão |
# MAGIC |---|---|---|
# MAGIC | Congelados | +0.47 | ✅ `add_regressor("producao_lag1")` |
# MAGIC | Padaria Fresca | +0.27 | ✅ `add_regressor("producao_lag1")` |
# MAGIC | Açougue & Peixaria | -0.05 | ❌ só tendência + sazonalidade |
# MAGIC | Laticínios & Ovos | +0.08 | ❌ só tendência + sazonalidade |
# MAGIC | Hortifruti | +0.11 | ❌ só tendência + sazonalidade |
# MAGIC | Frios & Embutidos | +0.03 | ❌ só tendência + sazonalidade |
# MAGIC
# MAGIC ## REGRA de ambiente (Databricks Serverless)
# MAGIC `spark.conf.set()` NÃO aceita configs `fs.azure.account.*`. Credenciais ADLS são passadas
# MAGIC via `.options(**adls_options)` em cada leitura, encapsulado no helper `ler_silver()`.

# COMMAND ----------

# MAGIC %pip install prophet statsmodels python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

# DBTITLE 1,Setup credenciais ADLS
import os
from dotenv import load_dotenv

ENV_PATH = "/Workspace/Users/ik.kukoo@gmail.com/.env"
load_dotenv(ENV_PATH, override=True)

def _require(key):
    v = os.getenv(key)
    if not v:
        raise EnvironmentError(f"Variável '{key}' não encontrada em {ENV_PATH}")
    return v

STORAGE_ACCOUNT = "internshipdatalake"

adls_options = {
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net":
        _require("ADLS_CLIENT_ID"),
    f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net":
        _require("ADLS_CLIENT_SECRET"),
    f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net":
        f"https://login.microsoftonline.com/{_require('ADLS_TENANT_ID')}/oauth2/token",
}

SILVER_SQUAD3   = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver"
FEATURES_BASE   = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/ml/features"

def ler_silver(nome_tabela):
    return spark.read.format("delta").options(**adls_options).load(f"{SILVER_SQUAD3}/{nome_tabela}")

print("✅ Setup OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros do split temporal (travados)
# MAGIC Alinhados com a EDA — não alterar sem combinar com Vinicius.

# COMMAND ----------

from datetime import date

DATA_FIM_TREINO = date(2026, 5, 31)   # treino inclui até 2026-05
DATA_FIM_TESTE  = date(2026, 8, 31)   # teste cobre 2026-06, 07, 08
DATA_FIM_FCST   = date(2026, 11, 30)  # forecast cobre 2026-09, 10, 11

# Categorias que recebem regressor de produção (decisão da EDA)
CATEGORIAS_COM_REGRESSOR = ["Congelados", "Padaria Fresca"]

print(f"Treino:   até {DATA_FIM_TREINO}")
print(f"Teste:    {date(2026, 6, 1)} → {DATA_FIM_TESTE}")
print(f"Forecast: {date(2026, 9, 1)} → {DATA_FIM_FCST}")
print(f"\nCategorias com regressor producao_lag1: {CATEGORIAS_COM_REGRESSOR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção do target `y`
# MAGIC Replicada aqui pra o notebook ser standalone

# COMMAND ----------

def build_target_y(spark):
    """
    Target y = SUM(quantidade) de perecíveis por categoria_pai × mês.
    Retorna DataFrame Spark (categoria_pai, ds, y).
    """
    df_itens = ler_silver("physical_itens_venda_caixa")
    df_perec = ler_silver("physical_produtos_pereciveis").select("sku", "categoria_pai")

    df_agregado = (
        df_itens.alias("i")
        .join(df_perec.alias("p"),
              F.col("i.codigo_barras_produto") == F.col("p.sku"), "inner")
        .filter(F.col("p.categoria_pai").isNotNull())
        .groupBy("p.categoria_pai", "i.ano", "i.mes")
        .agg(F.sum("i.quantidade").alias("y"))
        .select(
            F.col("categoria_pai"),
            F.make_date(F.col("ano"), F.col("mes"), F.lit(1)).alias("ds"),
            F.col("y").cast(DecimalType(18, 3)),
        )
    )

    bounds = df_agregado.agg(F.min("ds").alias("min_ds"), F.max("ds").alias("max_ds")).collect()[0]
    grid_meses = (
        spark.range(1).select(
            F.explode(
                F.sequence(F.lit(bounds["min_ds"]), F.lit(bounds["max_ds"]), F.expr("INTERVAL 1 MONTH"))
            ).alias("ds")
        )
    )
    categorias = df_agregado.select("categoria_pai").distinct()
    grid = grid_meses.crossJoin(categorias)

    return (
        grid.join(df_agregado, ["categoria_pai", "ds"], "left")
        .withColumn("y", F.coalesce(F.col("y"), F.lit(0).cast(DecimalType(18, 3))))
        .orderBy("categoria_pai", "ds")
    )

df_y = build_target_y(spark).cache()
print(f"✅ y construído: {df_y.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção da produção mensal por categoria (base do regressor)

# COMMAND ----------

df_lotes = ler_silver("food_lotes_producao")
df_perec = ler_silver("physical_produtos_pereciveis").select("sku", "categoria_pai")

df_producao_mensal = (
    df_lotes.alias("l")
    .join(df_perec.alias("p"), "sku", "inner")
    .filter(F.col("p.categoria_pai").isNotNull())
    .withColumn("ds", F.date_trunc("month", F.col("l.dt_fabricacao")).cast("date"))
    .groupBy("categoria_pai", "ds")
    .agg(
        F.sum("quantidade_produzida").alias("producao"),
        F.countDistinct("id_lote").alias("n_lotes"),
    )
)
display(df_producao_mensal.orderBy("categoria_pai", "ds"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montagem do dataset final — y + produção + lags

# COMMAND ----------

# Join y ⋈ producao, preenchendo meses sem produção com 0
df_base = (
    df_y
    .join(df_producao_mensal, ["categoria_pai", "ds"], "left")
    .fillna(0, subset=["producao", "n_lotes"])
)

# Lags de produção — ordenação por categoria_pai, ds
w_cat = Window.partitionBy("categoria_pai").orderBy("ds")

df_features = (
    df_base
    .withColumn("producao_lag1", F.lag("producao", 1).over(w_cat))
    .withColumn("producao_lag2", F.lag("producao", 2).over(w_cat))
    .withColumn("producao_lag3", F.lag("producao", 3).over(w_cat))
    .withColumn("producao_ma3",
                F.avg("producao").over(w_cat.rowsBetween(-3, -1)))
    # Features de calendário (para Prophet ou análise)
    .withColumn("mes",       F.month("ds"))
    .withColumn("trimestre", F.quarter("ds"))
    .withColumn("flag_fim_de_ano",  F.when(F.col("mes").isin(11, 12), 1).otherwise(0))
    .withColumn("flag_inicio_ano",  F.when(F.col("mes").isin(1, 2), 1).otherwise(0))
    # Preencher lags iniciais com 0 para não perder as primeiras linhas
    .fillna(0, subset=["producao_lag1", "producao_lag2", "producao_lag3", "producao_ma3"])
    .orderBy("categoria_pai", "ds")
)

print(f"Linhas do dataset de features: {df_features.count()}")
display(df_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marcar o split (treino / teste)
# MAGIC
# MAGIC Adiciona coluna `split` para facilitar o filtro no notebook 10.

# COMMAND ----------

df_features_split = (
    df_features
    .withColumn(
        "split",
        F.when(F.col("ds") <= F.lit(DATA_FIM_TREINO), F.lit("treino"))
         .when(F.col("ds") <= F.lit(DATA_FIM_TESTE),  F.lit("teste"))
         .otherwise(F.lit("fora"))  # não deveria haver dados aqui
    )
)

# Conferência da partição
print("Distribuição de linhas por split:")
df_features_split.groupBy("split").count().orderBy("split").show()

# Deve dar 29 meses × 6 cat = 174 em treino, 3 × 6 = 18 em teste
print("Por categoria × split (deve ser 29 treino / 3 teste):")
df_features_split.groupBy("categoria_pai", "split").count().orderBy("categoria_pai", "split").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Future frame — meses a prever (set, out, nov / 2026)
# MAGIC
# MAGIC Prophet exige um DataFrame de datas futuras. Como ele usa regressor em 2 categorias,
# MAGIC precisamos **projetar a produção futura** para essas 3 linhas (set, out, nov).
# MAGIC
# MAGIC **Estratégia: naive sazonal** — produção prevista para o mês M de 2026 = média da produção
# MAGIC do mesmo mês em 2024 e 2025. Captura o padrão sazonal sem complicar o pipeline.
# MAGIC Alternativas discutidas: média global da categoria (mais simples, menos acurado) ou
# MAGIC ajustar um Prophet sobre a própria produção (overengineering nesta iteração).

# COMMAND ----------

from datetime import datetime

# Projeção sazonal de produção pros 3 meses futuros
df_prod_projetada = (
    df_producao_mensal
    .withColumn("ano", F.year("ds"))
    .withColumn("mes", F.month("ds"))
    .filter(F.col("mes").isin(9, 10, 11))
    .groupBy("categoria_pai", "mes")
    .agg(F.avg("producao").alias("producao_proj"))
)
display(df_prod_projetada.orderBy("categoria_pai", "mes"))

# Grid future (3 meses × 6 categorias = 18 linhas)
future_meses = [date(2026, 9, 1), date(2026, 10, 1), date(2026, 11, 1)]
categorias_distintas = [r["categoria_pai"] for r in df_y.select("categoria_pai").distinct().collect()]

future_rows = [
    {"categoria_pai": c, "ds": d, "mes": d.month, "ano": d.year}
    for c in categorias_distintas for d in future_meses
]
df_future = spark.createDataFrame(future_rows)

# Juntar produção projetada e aplicar lag
# producao_lag1 em set/26 = produção projetada de ago/26 (que vem do df_features)
# Vamos derivar producao_lag1 juntando history + future

# Constrói uma série contínua de produção (history + projetada) e calcula lag1 por janela
df_prod_hist_proj = (
    df_features.select("categoria_pai", "ds", "producao")
    .unionByName(
        df_future.alias("f")
        .join(df_prod_projetada.alias("p"),
              (F.col("f.categoria_pai") == F.col("p.categoria_pai")) &
              (F.col("f.mes") == F.col("p.mes")),
              "left")
        .select(
            F.col("f.categoria_pai"),
            F.col("f.ds"),
            F.coalesce(F.col("p.producao_proj"), F.lit(0)).alias("producao"),
        )
    )
)

w_cat_full = Window.partitionBy("categoria_pai").orderBy("ds")
df_prod_com_lag = (
    df_prod_hist_proj
    .withColumn("producao_lag1", F.lag("producao", 1).over(w_cat_full))
    .fillna(0, subset=["producao_lag1"])
)

# df_future final: só os 3 meses futuros, com producao_lag1 derivado
df_future_final = (
    df_prod_com_lag
    .filter(F.col("ds") >= F.lit(date(2026, 9, 1)))
    .select("categoria_pai", "ds", "producao", "producao_lag1")
    .withColumn("split", F.lit("forecast"))
    .withColumn("mes", F.month("ds"))
)
print("Future frame (regressor pronto pro Prophet.predict):")
df_future_final.orderBy("categoria_pai", "ds").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistir os datasets
# MAGIC
# MAGIC Dois outputs:
# MAGIC - `ml/features/prophet_dataset/` — histórico (treino + teste) particionado por `categoria_pai`
# MAGIC - `ml/features/prophet_future_frame/` — frame futuro com regressores projetados

# COMMAND ----------

PATH_HIST   = f"{FEATURES_BASE}/prophet_dataset"
PATH_FUTURE = f"{FEATURES_BASE}/prophet_future_frame"

# Histórico — gravar particionado por categoria_pai para facilitar filtro no notebook 10
(
    df_features_split
    .filter(F.col("split") != "fora")
    .select("categoria_pai", "ds", "y", "producao", "producao_lag1", "producao_lag2",
            "producao_lag3", "producao_ma3", "n_lotes", "mes", "trimestre",
            "flag_fim_de_ano", "flag_inicio_ano", "split")
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("categoria_pai")
    .options(**adls_options)
    .save(PATH_HIST)
)
print(f"✅ Histórico salvo: {PATH_HIST}")

# Future frame — simples, sem partição (18 linhas)
(
    df_future_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .options(**adls_options)
    .save(PATH_FUTURE)
)
print(f"✅ Future frame salvo: {PATH_FUTURE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência final — ler de volta e validar contrato

# COMMAND ----------

df_hist_check = spark.read.format("delta").options(**adls_options).load(PATH_HIST)
df_fut_check  = spark.read.format("delta").options(**adls_options).load(PATH_FUTURE)

print("=== HISTÓRICO ===")
print(f"Linhas totais: {df_hist_check.count()} (esperado: 192)")
print("Por split:")
df_hist_check.groupBy("split").count().orderBy("split").show()
print("Colunas:", df_hist_check.columns)
df_hist_check.filter(F.col("categoria_pai") == "Congelados").orderBy("ds").show(5, truncate=False)

print("\n=== FUTURE FRAME ===")
print(f"Linhas totais: {df_fut_check.count()} (esperado: 18 = 6 cat × 3 meses)")
df_fut_check.orderBy("categoria_pai", "ds").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrato de saída (pro notebook 10)
# MAGIC
# MAGIC ### Dataset histórico (`prophet_dataset`)
# MAGIC | Coluna | Tipo | Observação |
# MAGIC |---|---|---|
# MAGIC | `categoria_pai` | string | **chave de partição** |
# MAGIC | `ds` | date | primeiro dia do mês (formato Prophet) |
# MAGIC | `y` | decimal(18,3) | target — soma de unidades vendidas |
# MAGIC | `producao` | decimal | produção do mês (mesma data — NÃO usar como regressor, lag0 é leakage) |
# MAGIC | `producao_lag1` | decimal | regressor a usar no Prophet |
# MAGIC | `producao_lag2`, `lag3`, `ma3` | decimal | opcionais, pra experimentação |
# MAGIC | `n_lotes` | long | contador de lotes no mês (feature extra possível) |
# MAGIC | `mes`, `trimestre` | int | features de calendário |
# MAGIC | `flag_fim_de_ano`, `flag_inicio_ano` | int | flags binários |
# MAGIC | `split` | string | `'treino'` ou `'teste'` |
# MAGIC
# MAGIC ### Future frame (`prophet_future_frame`)
# MAGIC | Coluna | Tipo |
# MAGIC |---|---|
# MAGIC | `categoria_pai` | string |
# MAGIC | `ds` | date (2026-09, 2026-10, 2026-11) |
# MAGIC | `producao` | decimal (projeção sazonal) |
# MAGIC | `producao_lag1` | decimal (regressor pronto pro Prophet.predict) |
# MAGIC | `split` | string ('forecast') |
# MAGIC | `mes` | int |
# MAGIC
# MAGIC ## Próximos passos
# MAGIC 1. Commit: `git add . && git commit -m "feat(ml): engenharia de features para Prophet (perecíveis)" && git push`
# MAGIC 2. Avisar Vinicius que os mesmos datasets estão em `abfss://squad3/ml/features/` — pode consumir pra consistência
# MAGIC 3. Abrir `10_prophet_treino.py` — treino em loop por categoria, avaliação WAPE/MAE, geração de forecast