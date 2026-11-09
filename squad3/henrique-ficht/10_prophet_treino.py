# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 · Treino Prophet — Modelo A (aditivo/sazonal) | Squad 3 | Henrique Ficht
# MAGIC
# MAGIC **Problema P1 · Previsão de demanda por categoria_pai (perecíveis)**
# MAGIC Comparação com Vinicius (Modelo B · XGBoost global)
# MAGIC
# MAGIC ## O que este notebook faz
# MAGIC 1. Lê os datasets gerados no `09_feature_engineering_ml.py`
# MAGIC 2. Treina 1 modelo Prophet por `categoria_pai` (loop de 6)
# MAGIC 3. Avalia no split de teste (últimos 3 meses) — WAPE e MAE
# MAGIC 4. Compara contra **baseline naive sazonal** (mesmo mês ano anterior)
# MAGIC 5. Gera o forecast dos próximos 3 meses (2026-09, 10, 11)
# MAGIC 6. Persiste predições e métricas em `squad3/ml/predictions/` pro Vinicius consumir
# MAGIC
# MAGIC ## Configuração Prophet (herdada da EDA)
# MAGIC - `growth='linear'` — trend plana
# MAGIC - `yearly_seasonality=True` — padrão anual claro
# MAGIC - `weekly_seasonality=False`, `daily_seasonality=False` — dados mensais
# MAGIC - `holidays=None` — zero outliers detectados
# MAGIC - `changepoint_prior_scale=0.05` — default
# MAGIC - `add_regressor('producao_lag1')` apenas em **Congelados** e **Padaria Fresca**
# MAGIC
# MAGIC ## Métricas
# MAGIC - **WAPE** (principal): `sum(|y - yhat|) / sum(y) * 100`
# MAGIC - **MAE** (secundária): `mean(|y - yhat|)`

# COMMAND ----------

# MAGIC %pip install prophet python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
from dotenv import load_dotenv
from prophet import Prophet
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType, TimestampType
)

# Silencia logs verbosos do cmdstanpy (Prophet)
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
logging.getLogger("prophet").setLevel(logging.ERROR)

plt.rcParams["figure.figsize"] = (12, 4)

# COMMAND ----------

# DBTITLE 1,Setup credenciais ADLS
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

FEATURES_BASE    = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/ml/features"
PREDICTIONS_BASE = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/ml/predictions"
MODELOS_BASE     = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/ml/modelos"

PATH_HIST   = f"{FEATURES_BASE}/prophet_dataset"
PATH_FUTURE = f"{FEATURES_BASE}/prophet_future_frame"

print("✅ Setup OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros do modelo

# COMMAND ----------

# Categorias que recebem o regressor (decisão da EDA — correlação lag1 positiva)
CATEGORIAS_COM_REGRESSOR = {"Congelados", "Padaria Fresca"}

PROPHET_PARAMS = dict(
    growth="linear",
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    seasonality_mode="additive",
    changepoint_prior_scale=0.05,
    interval_width=0.80,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar datasets

# COMMAND ----------

df_hist_spark   = spark.read.format("delta").options(**adls_options).load(PATH_HIST)
df_future_spark = spark.read.format("delta").options(**adls_options).load(PATH_FUTURE)

df_hist   = df_hist_spark.toPandas()
df_future = df_future_spark.toPandas()

# Prophet exige ds como datetime
df_hist["ds"]   = pd.to_datetime(df_hist["ds"])
df_future["ds"] = pd.to_datetime(df_future["ds"])
df_hist["y"]    = df_hist["y"].astype(float)
for col in ["producao", "producao_lag1", "producao_lag2", "producao_lag3", "producao_ma3"]:
    if col in df_hist.columns:
        df_hist[col] = df_hist[col].astype(float)
for col in ["producao", "producao_lag1"]:
    if col in df_future.columns:
        df_future[col] = df_future[col].astype(float)

categorias = sorted(df_hist["categoria_pai"].unique().tolist())
print(f"Categorias: {categorias}")
print(f"Histórico: {len(df_hist)} linhas | Future: {len(df_future)} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de métrica e baseline

# COMMAND ----------

def wape(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denom = np.sum(np.abs(y_true))
    if denom == 0:
        return np.nan
    return np.sum(np.abs(y_true - y_pred)) / denom * 100.0

def mae(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean(np.abs(y_true - y_pred)))

def baseline_sazonal_naive(df_cat_treino, datas_alvo):
    """
    Baseline: y_hat(mês M, ano A) = y(mês M, ano A-1).
    Se não existir A-1 para esse mês, cai pra média da categoria.
    """
    df_cat_treino = df_cat_treino.copy()
    df_cat_treino["ds"] = pd.to_datetime(df_cat_treino["ds"])
    media_cat = df_cat_treino["y"].mean()

    preds = []
    for d in datas_alvo:
        d_prev = d - pd.DateOffset(years=1)
        match = df_cat_treino.loc[df_cat_treino["ds"] == d_prev, "y"]
        preds.append(float(match.iloc[0]) if not match.empty else float(media_cat))
    return np.array(preds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop de treino + avaliação

# COMMAND ----------

def treinar_prophet_categoria(cat, df_cat_hist, df_cat_future, usa_regressor):
    """
    Treina um Prophet para uma categoria, avalia no teste e gera forecast.
    Retorna: (df_predict_teste, df_predict_forecast, metricas_dict)
    """
    df_treino = df_cat_hist[df_cat_hist["split"] == "treino"].sort_values("ds")
    df_teste  = df_cat_hist[df_cat_hist["split"] == "teste" ].sort_values("ds")

    # Prepara DataFrames no formato Prophet
    cols_prophet = ["ds", "y"] + (["producao_lag1"] if usa_regressor else [])
    df_fit = df_treino[cols_prophet].copy()

    # Monta DataFrame de predição = teste + forecast concatenados
    df_teste_pred = df_teste[["ds"]].copy()
    df_fcst_pred  = df_cat_future[["ds"]].sort_values("ds").copy()

    if usa_regressor:
        df_teste_pred["producao_lag1"] = df_teste["producao_lag1"].values
        df_fcst_pred["producao_lag1"]  = df_cat_future["producao_lag1"].values

    df_pred_full = pd.concat([df_teste_pred, df_fcst_pred], ignore_index=True)

    # Treina
    m = Prophet(**PROPHET_PARAMS)
    if usa_regressor:
        m.add_regressor("producao_lag1")
    m.fit(df_fit)

    # Prediz tudo (teste + forecast)
    forecast = m.predict(df_pred_full)

    # Separa teste e forecast dos resultados
    fcst_teste = forecast.iloc[:len(df_teste)].copy()
    fcst_fut   = forecast.iloc[len(df_teste):].copy()

    y_true_teste = df_teste["y"].values
    y_pred_teste = fcst_teste["yhat"].values

    # Métricas Prophet
    wape_prophet = wape(y_true_teste, y_pred_teste)
    mae_prophet  = mae(y_true_teste,  y_pred_teste)

    # Baseline sazonal naive
    y_pred_baseline = baseline_sazonal_naive(df_treino, df_teste["ds"].tolist())
    wape_baseline = wape(y_true_teste, y_pred_baseline)
    mae_baseline  = mae(y_true_teste,  y_pred_baseline)

    # Tabelas de saída
    df_predict_teste = pd.DataFrame({
        "categoria_pai": cat,
        "ds": df_teste["ds"].values,
        "y_real": y_true_teste,
        "y_prophet": y_pred_teste,
        "y_prophet_lower": fcst_teste["yhat_lower"].values,
        "y_prophet_upper": fcst_teste["yhat_upper"].values,
        "y_baseline_sazonal": y_pred_baseline,
        "usa_regressor": usa_regressor,
    })

    df_predict_forecast = pd.DataFrame({
        "categoria_pai": cat,
        "ds": df_cat_future["ds"].values,
        "y_prophet": fcst_fut["yhat"].values,
        "y_prophet_lower": fcst_fut["yhat_lower"].values,
        "y_prophet_upper": fcst_fut["yhat_upper"].values,
        "usa_regressor": usa_regressor,
    })

    metricas = {
        "categoria_pai": cat,
        "usa_regressor": usa_regressor,
        "n_treino": len(df_treino),
        "n_teste": len(df_teste),
        "wape_prophet": round(wape_prophet, 2),
        "mae_prophet":  round(mae_prophet, 2),
        "wape_baseline_sazonal": round(wape_baseline, 2),
        "mae_baseline_sazonal":  round(mae_baseline, 2),
        "ganho_wape_pp": round(wape_baseline - wape_prophet, 2),  # positivo = Prophet venceu
    }

    return df_predict_teste, df_predict_forecast, metricas, m

# COMMAND ----------

# Executa o loop
preds_teste_lst    = []
preds_forecast_lst = []
metricas_lst       = []
modelos            = {}

for cat in categorias:
    usa_reg = cat in CATEGORIAS_COM_REGRESSOR
    df_cat_hist   = df_hist[df_hist["categoria_pai"] == cat].copy()
    df_cat_future = df_future[df_future["categoria_pai"] == cat].copy()

    print(f"▶️  Treinando {cat} ({'com' if usa_reg else 'sem'} regressor)...")
    pt, pf, met, m = treinar_prophet_categoria(cat, df_cat_hist, df_cat_future, usa_reg)
    preds_teste_lst.append(pt)
    preds_forecast_lst.append(pf)
    metricas_lst.append(met)
    modelos[cat] = m
    print(f"    WAPE Prophet: {met['wape_prophet']:>6.2f}%  |  "
          f"WAPE baseline: {met['wape_baseline_sazonal']:>6.2f}%  |  "
          f"ganho: {met['ganho_wape_pp']:+.2f}pp")

df_preds_teste    = pd.concat(preds_teste_lst,    ignore_index=True)
df_preds_forecast = pd.concat(preds_forecast_lst, ignore_index=True)
df_metricas       = pd.DataFrame(metricas_lst)

print("\n✅ Loop concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métricas consolidadas

# COMMAND ----------

# Visão por categoria
display(spark.createDataFrame(df_metricas))

# Agregado (não usar média simples — usar WAPE global ponderado)
y_true_all     = df_preds_teste["y_real"].values
y_pred_prophet = df_preds_teste["y_prophet"].values
y_pred_baseln  = df_preds_teste["y_baseline_sazonal"].values

print("\n=== RESUMO GLOBAL (agregando 6 categorias × 3 meses) ===")
print(f"WAPE Prophet    : {wape(y_true_all, y_pred_prophet):>6.2f}%")
print(f"WAPE Baseline   : {wape(y_true_all, y_pred_baseln):>6.2f}%")
print(f"MAE  Prophet    : {mae(y_true_all, y_pred_prophet):>8.2f}")
print(f"MAE  Baseline   : {mae(y_true_all, y_pred_baseln):>8.2f}")
print(f"\nGanho absoluto do Prophet vs baseline (WAPE global): "
      f"{wape(y_true_all, y_pred_baseln) - wape(y_true_all, y_pred_prophet):+.2f}pp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização — histórico + teste real/predito + forecast

# COMMAND ----------

fig, axes = plt.subplots(3, 2, figsize=(16, 12))
for ax, cat in zip(axes.flat, categorias):
    # Histórico completo
    h = df_hist[df_hist["categoria_pai"] == cat].sort_values("ds")
    treino = h[h["split"] == "treino"]
    teste  = h[h["split"] == "teste"]
    ax.plot(treino["ds"], treino["y"], label="treino (real)", color="#6c757d")
    ax.plot(teste["ds"],  teste["y"],  label="teste (real)",  color="#111", marker="o")

    # Teste predito
    pt = df_preds_teste[df_preds_teste["categoria_pai"] == cat].sort_values("ds")
    ax.plot(pt["ds"], pt["y_prophet"], label="teste (Prophet)", color="#1f77b4", marker="x")
    ax.plot(pt["ds"], pt["y_baseline_sazonal"], label="teste (baseline)",
            color="#ffbb33", linestyle="--")

    # Forecast
    pf = df_preds_forecast[df_preds_forecast["categoria_pai"] == cat].sort_values("ds")
    ax.plot(pf["ds"], pf["y_prophet"], label="forecast (Prophet)", color="#2ca02c", marker="^")
    ax.fill_between(pf["ds"], pf["y_prophet_lower"], pf["y_prophet_upper"],
                    alpha=0.15, color="#2ca02c", label="IC 80%")

    m = df_metricas[df_metricas["categoria_pai"] == cat].iloc[0]
    reg_tag = "✓reg" if m["usa_regressor"] else ""
    ax.set_title(f"{cat}  — WAPE Prophet {m['wape_prophet']}%  vs  baseline {m['wape_baseline_sazonal']}%  {reg_tag}")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistir resultados

# COMMAND ----------

# Schema explícito para evitar inferência incorreta
schema_teste = StructType([
    StructField("categoria_pai", StringType(), False),
    StructField("ds", TimestampType(), False),
    StructField("y_real", DoubleType(), True),
    StructField("y_prophet", DoubleType(), True),
    StructField("y_prophet_lower", DoubleType(), True),
    StructField("y_prophet_upper", DoubleType(), True),
    StructField("y_baseline_sazonal", DoubleType(), True),
    StructField("usa_regressor", StringType(), True),
])

df_preds_teste["usa_regressor"]    = df_preds_teste["usa_regressor"].astype(str)
df_preds_forecast["usa_regressor"] = df_preds_forecast["usa_regressor"].astype(str)

# Teste
(
    spark.createDataFrame(df_preds_teste)
    .withColumn("modelo", F.lit("prophet_A_aditivo_sazonal"))
    .withColumn("run_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("categoria_pai")
    .options(**adls_options)
    .save(f"{PREDICTIONS_BASE}/prophet_teste")
)

# Forecast
(
    spark.createDataFrame(df_preds_forecast)
    .withColumn("modelo", F.lit("prophet_A_aditivo_sazonal"))
    .withColumn("run_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("categoria_pai")
    .options(**adls_options)
    .save(f"{PREDICTIONS_BASE}/prophet_forecast")
)

# Métricas
(
    spark.createDataFrame(df_metricas)
    .withColumn("modelo", F.lit("prophet_A_aditivo_sazonal"))
    .withColumn("run_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .options(**adls_options)
    .save(f"{PREDICTIONS_BASE}/prophet_metricas")
)

print(f"✅ Gravado em:")
print(f"   {PREDICTIONS_BASE}/prophet_teste    (por categoria)")
print(f"   {PREDICTIONS_BASE}/prophet_forecast (por categoria)")
print(f"   {PREDICTIONS_BASE}/prophet_metricas (plano)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cruzamento final — cobertura de abastecimento
# MAGIC
# MAGIC Este é o **entregável pro negócio**: pra cada categoria × mês futuro,
# MAGIC compara a demanda prevista (`yhat`) contra a produção planejada/projetada.
# MAGIC `cobertura < 1` = risco de ruptura; `cobertura >> 1` = sobreprodução.

# COMMAND ----------

df_cobertura = (
    df_preds_forecast[["categoria_pai", "ds", "y_prophet"]]
    .merge(
        df_future[["categoria_pai", "ds", "producao"]],
        on=["categoria_pai", "ds"], how="left"
    )
    .assign(cobertura=lambda d: d["producao"] / d["y_prophet"])
)
df_cobertura["cobertura"] = df_cobertura["cobertura"].round(2)
df_cobertura = df_cobertura.sort_values(["categoria_pai", "ds"])
display(spark.createDataFrame(df_cobertura))

print("\nSinais pra reposição/produção:")
for _, row in df_cobertura.iterrows():
    cat = row["categoria_pai"]
    mes = row["ds"].strftime("%Y-%m")
    cob = row["cobertura"]
    if cob < 0.9:
        sinal = "⚠️  RISCO DE RUPTURA — produção abaixo da demanda prevista"
    elif cob < 1.1:
        sinal = "✅ Equilibrado"
    elif cob < 2:
        sinal = "ℹ️  Folga normal"
    else:
        sinal = "⚠️  Sobreprodução — revisar planejamento"
    print(f"   {cat:<22s} {mes}  cobertura={cob:>5.2f}  {sinal}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist de fechamento
# MAGIC
# MAGIC - [ ] Rodou fim a fim sem erro
# MAGIC - [ ] Tabela de métricas publicada em `squad3/ml/predictions/prophet_metricas`
# MAGIC - [ ] Forecast publicado em `squad3/ml/predictions/prophet_forecast`
# MAGIC - [ ] Avisar Vinicius que os mesmos paths de `features/` estão disponíveis — ele só escreve em
# MAGIC       `squad3/ml/predictions/xgboost_*` pra gente juntar os resultados num notebook comparativo
# MAGIC - [ ] Commit: `git commit -m "feat(ml): treino Prophet (modelo A) com avaliação e forecast"`
# MAGIC - [ ] Rodar o `11_comparacao_prophet_xgboost.py` quando o Vinicius publicar
# MAGIC
# MAGIC ## Pontos a destacar no painel pro tech lead
# MAGIC
# MAGIC 1. **Prophet vs baseline sazonal naive** — ganho de WAPE em cada categoria (destacar onde Prophet venceu)
# MAGIC 2. **Regressor de produção** só ajudou em Congelados e Padaria Fresca (comparar WAPE com e sem, se der tempo)
# MAGIC 3. **Cobertura de abastecimento** — tabela final com sinais operacionais pros próximos 3 meses
# MAGIC 4. **Comparação com Modelo B** (Vinicius) — WAPE/MAE lado a lado por categoria; expectativa:
# MAGIC    - Prophet domina nas 4 categorias de sazonalidade regular
# MAGIC    - XGBoost pode brigar em Congelados e Açougue (hipersazonais)