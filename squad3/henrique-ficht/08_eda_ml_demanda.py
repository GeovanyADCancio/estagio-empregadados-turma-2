# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 · EDA para Modelo de Previsão de Demanda (Prophet) — Squad 3 | Henrique Ficht
# MAGIC
# MAGIC **Problema P1 · Modelo A (aditivo/sazonal)** — comparação com Vinicius (Modelo B · XGBoost global)
# MAGIC
# MAGIC ## Objetivo da etapa
# MAGIC EDA para entender se os dados estão adequados ao Prophet, conhecer as tabelas e gerar
# MAGIC insights que vão orientar a engenharia de features (notebook `09`).
# MAGIC
# MAGIC ## Escopo travado (combinado com Vinicius)
# MAGIC
# MAGIC Target `y` = **unidades vendidas de PERECÍVEIS por `categoria_pai` × mês**.
# MAGIC
# MAGIC Fontes únicas:
# MAGIC - `squad3/silver/physical_itens_venda_caixa` — fato de venda física
# MAGIC - `squad3/silver/physical_produtos_pereciveis` — dim que resolve `categoria_pai` via `sku`
# MAGIC - `squad3/silver/food_lotes_producao` — produção (regressor + cruzamento de cobertura)
# MAGIC - `squad3/silver/food_fornecedores` — regressores de fornecedor (opcional)
# MAGIC
# MAGIC ### Justificativa de escopo (perecíveis-only)
# MAGIC 1. O cruzamento com `food_lotes_producao` (cobertura de abastecimento) só faz sentido
# MAGIC    em perecíveis — e-commerce não é produzido em lote.
# MAGIC 2. Taxonomias de `categoria_pai` entre e-commerce (ex.: "Limpeza", "HPC") e
# MAGIC    perecíveis (ex.: "Padaria Fresca", "Açougue & Peixaria") são incompatíveis.
# MAGIC 3. Match perfeito entre `physical_itens_venda_caixa.codigo_barras_produto` e
# MAGIC    `physical_produtos_pereciveis.sku` (765/765 perecíveis cobertos).
# MAGIC 4. Zero dependência externa — nenhuma ingestão nova necessária.
# MAGIC
# MAGIC ## Contrato do problema
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Target | SUM(quantidade) de perecíveis por `categoria_pai` × mês |
# MAGIC | Granularidade | Mensal (primeiro dia do mês) |
# MAGIC | Modelo | Prophet (um por categoria_pai, sazonalidade anual) |
# MAGIC | Split treino | até `max(data) − 3 meses` |
# MAGIC | Split teste | últimos 3 meses de histórico |
# MAGIC | Forecast | 3 meses à frente de `max(data)` |
# MAGIC | Métricas | WAPE (principal), MAE |

# COMMAND ----------

# MAGIC %pip install prophet statsmodels seaborn python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from statsmodels.tsa.seasonal import seasonal_decompose

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 4)

# COMMAND ----------

# DBTITLE 1,Setup credenciais ADLS (mesmo padrão do 07_ingestao_gold)
import os
from dotenv import load_dotenv

ENV_PATH = "/Workspace/Users/ik.kukoo@gmail.com/.env"
load_dotenv(ENV_PATH, override=True)

def _require(key):
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Variável '{key}' não encontrada em {ENV_PATH}")
    return value

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

SILVER_SQUAD3 = f"abfss://squad3@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver"

def ler_silver(nome_tabela):
    """Helper — aplica adls_options em cada leitura (NUNCA setar via spark.conf.set)."""
    path = f"{SILVER_SQUAD3}/{nome_tabela}"
    return spark.read.format("delta").options(**adls_options).load(path)

print("✅ Credenciais carregadas e helper ler_silver pronto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Travamento do target `y`
# MAGIC
# MAGIC Função única usada por Henrique e Vinicius — **não alterar sem combinar com ele**.
# MAGIC Garante que WAPE/MAE sejam diretamente comparáveis entre Modelo A (Prophet) e Modelo B (XGBoost).

# COMMAND ----------

def build_target_y(spark):
    """
    Target y = SUM(quantidade) de perecíveis por categoria_pai × mês.
    Retorna Spark DataFrame com colunas (categoria_pai, ds, y)
      - ds: primeiro dia do mês (date)
      - y:  soma de unidades vendidas (decimal)
    """
    df_itens = ler_silver("physical_itens_venda_caixa")
    df_perec = ler_silver("physical_produtos_pereciveis").select("sku", "categoria_pai")

    # Agregação por (categoria_pai, ano, mes)
    df_agregado = (
        df_itens.alias("i")
        .join(
            df_perec.alias("p"),
            F.col("i.codigo_barras_produto") == F.col("p.sku"),
            "inner",
        )
        .filter(F.col("p.categoria_pai").isNotNull())
        .groupBy("p.categoria_pai", "i.ano", "i.mes")
        .agg(F.sum("i.quantidade").alias("y"))
        .select(
            F.col("categoria_pai"),
            F.make_date(F.col("ano"), F.col("mes"), F.lit(1)).alias("ds"),
            F.col("y"),
        )
    )

    # Grid completo (categoria × mês) para preencher meses sem venda com 0
    bounds = df_agregado.agg(F.min("ds").alias("min_ds"), F.max("ds").alias("max_ds")).collect()[0]
    grid_meses = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(F.lit(bounds["min_ds"]), F.lit(bounds["max_ds"]), F.expr("INTERVAL 1 MONTH"))
            ).alias("ds")
        )
    )
    categorias = df_agregado.select("categoria_pai").distinct()
    grid = grid_meses.crossJoin(categorias)

    df_final = (
        grid.join(df_agregado, ["categoria_pai", "ds"], "left")
        .withColumn("y", F.coalesce(F.col("y"), F.lit(0).cast("decimal(18,3)")))
        .orderBy("categoria_pai", "ds")
    )
    return df_final


df_y = build_target_y(spark)
df_y.cache()
total = df_y.count()
print(f"✅ Target construído — {total:,} linhas (categoria_pai × mês)")
display(df_y.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Inventário e qualidade

# COMMAND ----------

# Visão geral do y
df_y.describe("y").show()

bounds = df_y.agg(
    F.min("ds").alias("primeiro_mes"),
    F.max("ds").alias("ultimo_mes"),
    F.countDistinct("ds").alias("n_meses"),
    F.countDistinct("categoria_pai").alias("n_categorias"),
).collect()[0]
print(f"Primeiro mês: {bounds['primeiro_mes']}")
print(f"Último mês:   {bounds['ultimo_mes']}")
print(f"Nº meses:     {bounds['n_meses']}")
print(f"Nº categorias: {bounds['n_categorias']}")

# COMMAND ----------

# Qualidade da fonte — physical_itens_venda_caixa
df_itens = ler_silver("physical_itens_venda_caixa")
print(f"physical_itens_venda_caixa: {df_itens.count():,} linhas")
print(f"  Período: {df_itens.agg(F.min('ano')).first()[0]}-{df_itens.agg(F.min('mes')).first()[0]:02d}  "
      f"→  {df_itens.agg(F.max('ano')).first()[0]}-{df_itens.agg(F.max('mes')).first()[0]:02d}")
print(f"  Nulos em quantidade: {df_itens.filter(F.col('quantidade').isNull()).count():,}")
print(f"  Flag valor_inconsistente: {df_itens.filter(F.col('flag_valor_inconsistente')).count():,}")
print(f"  Códigos distintos: {df_itens.select('codigo_barras_produto').distinct().count():,}")

# COMMAND ----------

# Qualidade da dim — physical_produtos_pereciveis
df_perec = ler_silver("physical_produtos_pereciveis")
print(f"physical_produtos_pereciveis: {df_perec.count():,} SKUs")
print(f"  Categoria_pai distintas: {df_perec.select('categoria_pai').distinct().count()}")
display(df_perec.groupBy("categoria_pai").count().orderBy(F.desc("count")))

# COMMAND ----------

# Cobertura do join — quantas vendas ficam fora do modelo (não-perecíveis)?
total_linhas_itens = df_itens.count()
linhas_pereciveis = (
    df_itens.alias("i")
    .join(df_perec.alias("p"), F.col("i.codigo_barras_produto") == F.col("p.sku"), "inner")
    .count()
)
print(f"Vendas totais:     {total_linhas_itens:,}")
print(f"Vendas perecíveis: {linhas_pereciveis:,} ({100*linhas_pereciveis/total_linhas_itens:.1f}%)")
print(f"Vendas excluídas (não perecíveis): {total_linhas_itens - linhas_pereciveis:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Caracterização do target por `categoria_pai`
# MAGIC
# MAGIC **Decidir aqui** quais categorias entram no Prophet. Regra: ≥18 meses com venda real
# MAGIC (ideal ≥24 para capturar sazonalidade anual com folga).

# COMMAND ----------

hist_por_cat = (
    df_y
    .filter(F.col("y") > 0)
    .groupBy("categoria_pai")
    .agg(
        F.count("*").alias("n_meses_com_venda"),
        F.min("ds").alias("primeiro_mes"),
        F.max("ds").alias("ultimo_mes"),
        F.sum("y").alias("total_vendas"),
        F.avg("y").alias("media_mensal"),
        F.stddev("y").alias("desvio_mensal"),
    )
    .withColumn("cv", F.col("desvio_mensal") / F.col("media_mensal"))
    .orderBy(F.desc("total_vendas"))
)
display(hist_por_cat)

# COMMAND ----------

MIN_MESES = 18
elegiveis = [
    r["categoria_pai"]
    for r in hist_por_cat.filter(F.col("n_meses_com_venda") >= MIN_MESES).collect()
]
descartadas = [
    r["categoria_pai"]
    for r in hist_por_cat.filter(F.col("n_meses_com_venda") < MIN_MESES).collect()
]
print(f"✅ Elegíveis para Prophet ({len(elegiveis)}): {elegiveis}")
print(f"❌ Descartadas / baseline ({len(descartadas)}): {descartadas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Padrões temporais
# MAGIC
# MAGIC Visualizar a série de cada categoria elegível e decompor em tendência + sazonalidade + resíduo.
# MAGIC Marcar outliers que vão virar **feriados customizados** no Prophet.

# COMMAND ----------

df_y_pd = df_y.toPandas()
df_y_pd["ds"] = pd.to_datetime(df_y_pd["ds"])
df_y_pd["y"] = df_y_pd["y"].astype(float)

# COMMAND ----------

# Facet grid — série temporal por categoria elegível
sub = df_y_pd[df_y_pd["categoria_pai"].isin(elegiveis)]
g = sns.FacetGrid(sub, col="categoria_pai", col_wrap=3, height=3, sharey=False, aspect=1.4)
g.map_dataframe(sns.lineplot, x="ds", y="y")
g.set_titles("{col_name}")
g.set_xlabels("")
g.set_ylabels("unidades")
for ax in g.axes.flat:
    ax.tick_params(axis="x", rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

def decompor(df_pd, categoria):
    serie = (
        df_pd[df_pd["categoria_pai"] == categoria]
        .set_index("ds")["y"]
        .asfreq("MS")
        .fillna(0)
    )
    if len(serie) < 24:
        return None
    return seasonal_decompose(serie, model="additive", period=12)

# Exemplo: decomposição da primeira categoria elegível
if elegiveis:
    cat = elegiveis[0]
    decomp = decompor(df_y_pd, cat)
    if decomp is not None:
        fig = decomp.plot()
        fig.set_size_inches(12, 8)
        plt.suptitle(f"Decomposição aditiva — {cat}", y=1.02)
        plt.tight_layout()
        plt.show()
    else:
        print(f"⚠️  {cat}: histórico < 24 meses — decomposição anual não disponível")

# COMMAND ----------

# Resumo da força sazonal por categoria
print(f"{'categoria_pai':<40s}  amplitude_sazonal / nível")
print("-" * 70)
for cat in elegiveis:
    decomp = decompor(df_y_pd, cat)
    if decomp is None:
        print(f"{cat:<40s}  (histórico curto)")
        continue
    seas_amp = decomp.seasonal.max() - decomp.seasonal.min()
    nivel = decomp.trend.dropna().mean()
    ratio = seas_amp / nivel if nivel else float("nan")
    print(f"{cat:<40s}  {ratio:>8.2%}")

# COMMAND ----------

# Outliers via resíduo (|z| > 3) — candidatos a feriados customizados
def outliers_categoria(df_pd, categoria):
    decomp = decompor(df_pd, categoria)
    if decomp is None:
        return []
    resid = decomp.resid.dropna()
    z = (resid - resid.mean()) / resid.std()
    return resid[z.abs() > 3].index.strftime("%Y-%m").tolist()

print("Outliers detectados (candidatos a feriado customizado):")
for cat in elegiveis:
    dates = outliers_categoria(df_y_pd, cat)
    if dates:
        print(f"  {cat}: {dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Relação oferta × demanda
# MAGIC
# MAGIC `food_lotes_producao` será o principal regressor extra no Prophet.
# MAGIC Checar: (a) produção acompanha vendas? (b) lag de produção prediz demanda futura?

# COMMAND ----------

df_lotes = ler_silver("food_lotes_producao")
df_lotes_perec = (
    df_lotes.alias("l")
    .join(df_perec.select("sku", "categoria_pai").alias("p"), "sku", "inner")
    .filter(F.col("p.categoria_pai").isNotNull())
    .withColumn("ds", F.date_trunc("month", F.col("l.dt_fabricacao")).cast("date"))
    .groupBy("categoria_pai", "ds")
    .agg(
        F.sum("quantidade_produzida").alias("producao"),
        F.countDistinct("id_lote").alias("n_lotes"),
    )
)

df_oferta_demanda = (
    df_y.join(df_lotes_perec, ["categoria_pai", "ds"], "left")
    .fillna(0, subset=["producao", "n_lotes"])
    .withColumn(
        "cobertura",
        F.when(F.col("y") > 0, F.col("producao") / F.col("y")).otherwise(None),
    )
)
display(df_oferta_demanda.orderBy("categoria_pai", "ds"))

# COMMAND ----------

# Scatter produção × vendas por categoria
od_pd = df_oferta_demanda.toPandas()
od_pd["ds"] = pd.to_datetime(od_pd["ds"])
od_pd["y"] = od_pd["y"].astype(float)
od_pd["producao"] = od_pd["producao"].astype(float)

sub = od_pd[od_pd["categoria_pai"].isin(elegiveis)]
g = sns.FacetGrid(sub, col="categoria_pai", col_wrap=3, height=3, sharex=False, sharey=False)
g.map_dataframe(sns.scatterplot, x="producao", y="y", alpha=0.6)
g.set_titles("{col_name}")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Lag correlation — produção de M-k prediz vendas em M?
def lag_corr(df_pd, categoria, max_lag=3):
    sub = df_pd[df_pd["categoria_pai"] == categoria].sort_values("ds").reset_index(drop=True)
    return {
        f"producao_lag{lag}": round(sub["y"].corr(sub["producao"].shift(lag)), 3)
        for lag in range(0, max_lag + 1)
    }

lag_df = pd.DataFrame([{"categoria_pai": c, **lag_corr(od_pd, c)} for c in elegiveis])
display(lag_df)

# COMMAND ----------

# Cobertura histórica média
cobertura_media = (
    df_oferta_demanda
    .groupBy("categoria_pai")
    .agg(F.avg("cobertura").alias("cobertura_media"))
    .orderBy("cobertura_media")
)
display(cobertura_media)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Insights consolidados
# MAGIC
# MAGIC ### Qualidade dos dados
# MAGIC - **Período coberto:** 2024-01 a 2026-08 (32 meses × 6 categorias = 192 linhas, grid 100% cheio)
# MAGIC - **Nota:** dados de set-out-nov/2026 ainda não foram ingeridos na Silver — esperado, ciclo em andamento
# MAGIC - **% de vendas excluídas por serem não-perecíveis:** 49.2% (1.753.201 / 3.562.643) — alinhado à expectativa
# MAGIC - **Zero nulos** em `quantidade`, **zero flags** `valor_inconsistente` — Silver limpa
# MAGIC - **765 SKUs perecíveis** cobrem 100% da dim `physical_produtos_pereciveis`
# MAGIC
# MAGIC ### Decisão sobre categorias
# MAGIC - ✅ **Entram no Prophet (6/6):** Laticínios & Ovos, Congelados, Açougue & Peixaria, Frios & Embutidos, Hortifruti, Padaria Fresca — todas com 32 meses de histórico (muito acima do mínimo de 18)
# MAGIC - ❌ Nenhuma descartada
# MAGIC
# MAGIC ### Sazonalidade — dois tiers de força, padrão unificado
# MAGIC - **Hipersazonais (amplitude > nível):** Congelados (167%), Açougue & Peixaria (162%)
# MAGIC - **Alta regular (~60-65%):** Laticínios, Frios, Hortifruti, Padaria Fresca
# MAGIC - **Padrão unificado:** pico em set-nov, vale em dez-fev. Dois ciclos consecutivos (2024-25 e 2025-26) quase idênticos — Prophet vai aprender isso lindamente
# MAGIC
# MAGIC ### Tendência — PLANA
# MAGIC - Decomposição de Laticínios mostra trend oscilando entre 24.150 e 24.400 (<1% de variação em 2,5 anos)
# MAGIC - Visualmente, nenhuma categoria mostra crescimento ou queda relevante
# MAGIC - **Configuração Prophet:** `growth='linear'`, `changepoint_prior_scale=0.05` (default)
# MAGIC
# MAGIC ### Feriados customizados — não necessários
# MAGIC - **Zero outliers** detectados (|z| > 3) em qualquer categoria
# MAGIC - Modelo "tendência + sazonalidade anual" sozinho explica praticamente todo o sinal
# MAGIC - **Decisão:** não passar `holidays` ao Prophet nesta primeira iteração
# MAGIC
# MAGIC ### Oferta × demanda — lag1 é o regressor viável
# MAGIC | Categoria | Melhor lag | Correlação | Uso no modelo |
# MAGIC |---|---|---|---|
# MAGIC | Congelados | lag1 | +0.47 | ✅ `add_regressor("producao_lag1")` |
# MAGIC | Padaria Fresca | lag1 | +0.27 | ✅ `add_regressor("producao_lag1")` |
# MAGIC | Açougue & Peixaria | lag0 | +0.84 | ⚠️ Lag0 é data leakage pro forecast — descartar |
# MAGIC | Laticínios & Ovos | — | — | Sem regressor útil |
# MAGIC | Hortifruti | — | — | Sem regressor útil |
# MAGIC | Frios & Embutidos | — | — | Sem regressor útil |
# MAGIC
# MAGIC - **Cobertura histórica média: 1.9 a 3.0** — produção foi sempre 2-3x maior que vendas. Nenhuma categoria em ruptura