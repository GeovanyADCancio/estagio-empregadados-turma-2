# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %pip install pymssql

# COMMAND ----------

# MAGIC %md # Gold — Squad 3 (`physical_lojas` + `physical_produtos_pereciveis`)
# MAGIC
# MAGIC Este notebook transforma a Silver em **tabelas prontas para o BI** (KPIs, dimensões e validações).
# MAGIC Ele está dividido em **6 partes e 20 etapas** — cada etapa tem uma explicação logo acima do código.
# MAGIC
# MAGIC | Parte | Etapas | O que acontece |
# MAGIC |---|---|---|
# MAGIC | 1. Preparação | 1 – 6 | Imports, credenciais, caminhos, schemas e funções reutilizáveis |
# MAGIC | 2. Carga e limpeza | 7 – 8 | Lê Silver/Raw e aplica a limpeza mínima nas dependências |
# MAGIC | 3. KPIs de lojas | 9 – 12 | Dimensão de lojas, visão mensal, anual e semestral |
# MAGIC | 4. KPIs de perecíveis | 13 – 17 | Dimensão de produtos, SKUs ativos, preço, validade e checagem de SKU |
# MAGIC | 5. Documentação e gravação | 18 – 19 | Dicionário de dados e gravação com MERGE no Delta (`gold/`) e no SQL Server |
# MAGIC | 6. Validações | 20 | Confere se a Gold gravada está completa e consistente |
# MAGIC
# MAGIC **Fluxo dos dados**
# MAGIC ```
# MAGIC Silver: lojas, produtos, lotes ─────────────┐
# MAGIC Silver (ou Raw temporária): vendas, itens,  ├─► limpeza mínima ─► KPIs ─► MERGE: Delta gold/ + SQL Server ─► validações
# MAGIC   estoque, ecommerce_produtos ──────────────┘
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 1 — Preparação
# MAGIC
# MAGIC Nada é lido nem gravado aqui: só deixamos tudo configurado.

# COMMAND ----------

# MAGIC %md ### Etapa 1 — Importações
# MAGIC
# MAGIC **O que faz**
# MAGIC - Importa as funções do PySpark (`F`), janelas (`Window`) e os tipos usados nos schemas.

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 1: IMPORTAÇÕES


from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, DateType, BooleanType
)
from delta.tables import DeltaTable
import sys, subprocess


# COMMAND ----------

# MAGIC %md ### Etapa 2 — Credenciais de acesso ao Data Lake
# MAGIC
# MAGIC **O que faz**
# MAGIC - Monta o dicionário `config`, usado em todas as leituras e escritas.
# MAGIC - Tenta registrar as credenciais também na sessão Spark — se o cluster bloquear, só mostra um aviso.
# MAGIC

# COMMAND ----------

import os
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("ADLS_CLIENT_ID")
tenant_id = os.getenv("ADLS_TENANT_ID")
client_secret = os.getenv("ADLS_CLIENT_SECRET")
storage_account_name = os.getenv(
    "ADLS_STORAGE_ACCOUNT_NAME",
    "internshipdatalake"
)

if not all([client_id, tenant_id, client_secret]):
    raise ValueError("Credenciais Azure não configuradas.")

# COMMAND ----------

# MAGIC %md ### Etapa 3 — Caminhos e parâmetros
# MAGIC
# MAGIC **O que faz**
# MAGIC - Define onde está cada tabela: Bronze, Silver, Raw e a pasta `gold/`.
# MAGIC - Para vendas, itens, estoque e e-commerce há **dois caminhos** (Silver e Raw)
# MAGIC - `FALHAR_SE_INCONSISTENTE`: se `True`, o notebook para quando a Etapa 20 encontra problema.
# MAGIC - **SQL Server:** servidor, banco, schema, usuário e senha. `ENVIAR_SQL_SERVER = False` desliga o envio.

# COMMAND ----------

# ETAPA 3 — INVENTÁRIO DA SILVER

base_squad3 = f"abfss://squad3@{host}/"

tabelas_silver = [
    "physical_lojas",
    "physical_produtos_pereciveis",
    "food_lotes_producao",
    "physical_vendas_caixa",
    "physical_itens_venda_caixa",
    "food_estoque_lojas",
    "ecommerce_produtos"
]

resultado = []

for tabela in tabelas_silver:
    caminho = f"{base_squad3}silver/{tabela}"

    try:
        df = spark.read.format("delta").options(**config).load(caminho)
        df.limit(1).collect()

        resultado.append((tabela, "DISPONÍVEL"))

        print(f"\nTabela: {tabela}")
        print("Colunas:", df.columns)
        display(df.limit(10))

    except Exception as e:
        resultado.append((tabela, "NÃO CONFIRMADA"))
        print(f"\n{tabela}: {type(e).__name__} — {str(e)[:200]}")

df_inventario = spark.createDataFrame(
    resultado,
    ["tabela", "status"]
)

print("\nRESUMO DA SILVER")
display(df_inventario)

# COMMAND ----------

# ETAPA 4 — VERIFICAR AS FONTES NA RAW

base_raw = f"abfss://raw@{host}/batch-data/"

arquivos = {
    "physical_vendas_caixa": base_raw + "physical_vendas_caixa.csv",
    "ecommerce_produtos": base_raw + "ecommerce_produtos.csv"
}

for nome, caminho in arquivos.items():
    print(f"\n===== {nome} =====")

    try:
        df_teste = (
            spark.read
            .format("csv")
            .options(**config)
            .option("header", "true")
            .load(caminho)
        )

        df_teste.limit(1).collect()

        print("ARQUIVO DISPONÍVEL")
        print("Caminho:", caminho)

        df_teste.printSchema()
        display(df_teste.limit(10))

    except Exception as e:
        print("LEITURA NÃO CONFIRMADA")
        print("Erro:", type(e).__name__)
        print(str(e)[:300])

# COMMAND ----------

# ETAPA 5 — VERIFICAR TABELAS NA BRONZE

base_bronze = f"abfss://squad3@{host}/bronze/"

tabelas = [
    "physical_vendas_caixa",
    "ecommerce_produtos"
]

for nome in tabelas:
    caminho = base_bronze + nome

    print(f"\n===== {nome} =====")

    try:
        df = (
            spark.read.format("delta")
            .options(**config)
            .load(caminho)
        )

        df.limit(1).collect()

        print("DISPONÍVEL NA BRONZE")
        df.printSchema()
        display(df.limit(10))

    except Exception as e:
        print("NÃO CONFIRMADA NA BRONZE")
        print(type(e).__name__, str(e)[:250])

# COMMAND ----------

# ETAPA 6 — LOCALIZAR TABELAS NO CATÁLOGO

tabelas = spark.sql("""
    SELECT
        table_catalog,
        table_schema,
        table_name
    FROM system.information_schema.tables
    WHERE lower(table_name) LIKE '%physical_vendas_caixa%'
       OR lower(table_name) LIKE '%ecommerce_produtos%'
""")

display(tabelas)

# COMMAND ----------

# ETAPA 7 — BUSCAR TABELAS NA SILVER DOS SQUADS

tabelas = [
    "physical_vendas_caixa",
    "ecommerce_produtos"
]

resultados = []

for squad in ["squad1", "squad2", "squad3", "squad4"]:
    for tabela in tabelas:

        caminho = f"abfss://{squad}@{host}/silver/{tabela}"

        try:
            df = (
                spark.read.format("delta")
                .options(**config)
                .load(caminho)
            )

            df.limit(1).collect()

            resultados.append((squad, tabela, "DISPONÍVEL", caminho))

        except Exception as e:
            resultados.append(
                (squad, tabela, "NÃO CONFIRMADA", caminho)
            )

df_busca = spark.createDataFrame(
    resultados,
    ["squad", "tabela", "status", "caminho"]
)

display(df_busca)

# COMMAND ----------

# ETAPA 8 — COMPARAR ECOMMERCE DOS SQUADS 1 E 2

for squad in ["squad1", "squad2"]:

    caminho = f"abfss://{squad}@{host}/silver/ecommerce_produtos"

    df = (
        spark.read.format("delta")
        .options(**config)
        .load(caminho)
    )

    print(f"\n===== {squad.upper()} =====")
    print("Total de registros:", df.count())

    df.printSchema()
    display(df.limit(10))

# COMMAND ----------

# ETAPA 9 — COMPARAR QUALIDADE DAS SILVER

from pyspark.sql import functions as F

resultados = []

for squad in ["squad1", "squad2"]:

    caminho = f"abfss://{squad}@{host}/silver/ecommerce_produtos"

    df = (
        spark.read.format("delta")
        .options(**config)
        .load(caminho)
    )

    total = df.count()

    skus_distintos = (
        df.filter(F.col("sku").isNotNull())
          .select("sku")
          .distinct()
          .count()
    )

    skus_nulos = df.filter(
        F.col("sku").isNull()
    ).count()

    precos_invalidos = df.filter(
        F.col("preco_lista").isNull() |
        (F.col("preco_lista") <= 0)
    ).count()

    resultados.append((
        squad,
        total,
        skus_distintos,
        total - skus_distintos - skus_nulos,
        skus_nulos,
        precos_invalidos
    ))

df_comparacao = spark.createDataFrame(
    resultados,
    [
        "squad",
        "total_registros",
        "skus_distintos",
        "registros_duplicados_sku",
        "skus_nulos",
        "precos_invalidos"
    ]
)

display(df_comparacao)

# COMMAND ----------

# ETAPA 10 — INSPECIONAR VENDAS NA RAW

caminho_vendas = (
    f"abfss://raw@{host}/batch-data/physical_vendas_caixa.csv"
)

df_vendas_raw = (
    spark.read
    .format("csv")
    .options(**config)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(caminho_vendas)
)

print("Total de registros:", df_vendas_raw.count())

df_vendas_raw.printSchema()

display(df_vendas_raw.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

df_validacao_vendas = df_vendas_raw.agg(
    F.count("*").alias("total_registros"),
    F.countDistinct("id_transacao").alias("transacoes_distintas"),
    F.sum(
        F.when(F.col("id_transacao").isNull(), 1).otherwise(0)
    ).alias("transacoes_sem_id"),
    F.sum(
        F.when(F.col("id_loja").isNull(), 1).otherwise(0)
    ).alias("lojas_nulas"),
    F.sum(
        F.when(F.col("dt_venda").isNull(), 1).otherwise(0)
    ).alias("datas_nulas"),
    F.sum(
        F.when(
            F.col("valor_total_venda").isNull() |
            (F.col("valor_total_venda") <= 0),
            1
        ).otherwise(0)
    ).alias("valores_invalidos"),
    F.min("dt_venda").alias("primeira_venda"),
    F.max("dt_venda").alias("ultima_venda")
)

display(df_validacao_vendas)

# COMMAND ----------

# MAGIC %md ### Etapa 4 — Schemas da Raw
# MAGIC
# MAGIC **O que faz**
# MAGIC - Declara as colunas e tipos dos CSVs da Raw em vez de usar `inferSchema`.
# MAGIC - Deixa a leitura mais rápida (3,5 mi de itens) e mantém o CPF como texto (não perde zero à esquerda).

# COMMAND ----------

# ============================================================
# SCHEMAS EXPLÍCITOS DA RAW (evita inferSchema em 3,5 mi linhas
# e preserva zeros à esquerda de CPF)
# ============================================================

from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DoubleType,
    TimestampType, DateType, BooleanType
)
schema_vendas = StructType([
    StructField("id_transacao", StringType()),
    StructField("id_loja", IntegerType()),
    StructField("id_caixa", IntegerType()),
    StructField("id_operador", IntegerType()),
    StructField("dt_venda", TimestampType()),
    StructField("valor_total_venda", DoubleType()),
    StructField("cpf_cliente", StringType()),
    StructField("tipo_pagamento", StringType()),
])

schema_itens = StructType([
    StructField("id_item_venda", LongType()),
    StructField("id_transacao", StringType()),
    StructField("codigo_barras_produto", StringType()),
    StructField("quantidade", DoubleType()),
    StructField("preco_unitario_registro", DoubleType()),
    StructField("valor_total_item", DoubleType()),
])

schema_estoque = StructType([
    StructField("id_loja", IntegerType()),
    StructField("sku", StringType()),
    StructField("id_lote", DoubleType()),   # vem como "123.0" no CSV
    StructField("quantidade_disponivel", IntegerType()),
    StructField("estoque_minimo", IntegerType()),
    StructField("dt_snapshot", DateType()),
])

schema_ecommerce = StructType([
    StructField("sku", StringType()),
    StructField("nome_produto", StringType()),
    StructField("descricao", StringType()),
    StructField("id_categoria", IntegerType()),
    StructField("preco_lista", DoubleType()),
    StructField("unidade_medida", StringType()),
    StructField("nome_marca", StringType()),
    StructField("is_ativo", BooleanType()),
])

schema_ibge = StructType([
    StructField("municipio", StringType()),
    StructField("uf", StringType()),
    StructField("populacao", LongType()),
    StructField("renda_per_capita", DoubleType()),
])


# COMMAND ----------

# MAGIC %md ### Etapa 5 — Funções de leitura e escrita
# MAGIC
# MAGIC **O que faz**
# MAGIC - `ler_delta` / `ler_csv`: leem uma tabela Delta ou um CSV.
# MAGIC - `existe_delta` / `existe_csv`: testam se o caminho existe (sem usar `DeltaTable`).
# MAGIC - `ler_silver_ou_raw`: usa a Silver se existir; senão, a Raw. Guarda a fonte usada em `FONTES`.
# MAGIC - `salvar_gold`: adiciona `gold_processed_at` e grava nos **dois destinos** (Delta e SQL Server), anotando o resultado em `RESUMO_GRAVACAO`.
# MAGIC

# COMMAND ----------

# ============================================================
# ETAPA 5 — FUNÇÕES DE LEITURA E GRAVAÇÃO
# GOLD SQUAD 3 | ADLS GEN2
# ============================================================

from pyspark.sql import functions as F

FONTES = {}
RESUMO_GRAVACAO = []

# Segurança: gravação desabilitada inicialmente
EXECUTAR_GRAVACAO = False

# SQL Server permanece desabilitado
ENVIAR_SQL_SERVER = False


# ------------------------------------------------------------
# LEITURA DAS FONTES
# ------------------------------------------------------------

def ler_delta(caminho):
    return (
        spark.read
        .format("delta")
        .options(**config)
        .load(caminho)
    )


def ler_csv(caminho, schema):
    return (
        spark.read
        .format("csv")
        .options(**config)
        .option("header", "true")
        .schema(schema)
        .load(caminho)
    )


def ler_fonte_silver(nome, caminho):
    df = ler_delta(caminho)
    df.limit(1).collect()

    FONTES[nome] = "SILVER"
    return df


def ler_fonte_raw(nome, caminho, schema):
    df = ler_csv(caminho, schema)
    df.limit(1).collect()

    FONTES[nome] = "RAW"
    return df


# ------------------------------------------------------------
# GRAVAÇÃO GOLD — REPROCESSAMENTO COMPLETO
# ------------------------------------------------------------

def salvar_gold(df, nome, chaves, particoes=None):

    if not EXECUTAR_GRAVACAO:
        raise RuntimeError(
            "Gravação Gold bloqueada. "
            "Valide os dados antes de habilitar."
        )

    # Impede escrita fora da Gold do Squad 3
    caminho = base_gold + nome

    if not caminho.startswith(
        "abfss://squad3@internshipdatalake.dfs.core.windows.net/gold/"
    ):
        raise ValueError("Destino não autorizado.")

    # Confere integridade das chaves
    total = df.count()
    distintas = df.select(*chaves).distinct().count()

    if total != distintas:
        raise ValueError(
            f"{nome}: existem chaves duplicadas."
        )

    filtro_nulos = " OR ".join(
        f"`{chave}` IS NULL" for chave in chaves
    )

    if df.filter(F.expr(filtro_nulos)).limit(1).count() > 0:
        raise ValueError(
            f"{nome}: existem chaves nulas."
        )

    # Acrescenta data de processamento
    df_final = df.withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )

    # Substitui integralmente a tabela Gold
    writer = (
        df_final.write
        .format("delta")
        .options(**config)
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if particoes:
        writer = writer.partitionBy(*particoes)

    writer.save(caminho)

    RESUMO_GRAVACAO.append(
        (nome, ", ".join(chaves), "overwrite", "desligado")
    )

    print(f"Gold gravada: {nome} | {total} registros")

# COMMAND ----------

# MAGIC %md ### Etapa 6 — Funções auxiliares
# MAGIC
# MAGIC **O que faz**
# MAGIC - `normalizar_texto`: tira acento, espaço extra e põe em maiúsculo (para cruzar nomes de cidade).
# MAGIC - `pct`: calcula variação percentual e devolve nulo se a base for 0 ou nula.
# MAGIC - `dedup`: mantém 1 linha por chave, escolhendo de forma determinística.

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 6: FUNÇÕES AUXILIARES

from pyspark.sql import functions as F
from pyspark.sql.window import Window

_ACENTOS = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_SEM_ACENTOS = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def normalizar_texto(c):
    """Upper, trim, sem acento e com espaços simples - chave de join por nome."""
    return F.upper(
        F.regexp_replace(F.trim(F.translate(c, _ACENTOS, _SEM_ACENTOS)), r"\s+", " ")
    )


def pct(numerador, denominador):
    return F.when(
        denominador.isNull() | (denominador == 0), F.lit(None)
    ).otherwise(F.round((numerador - denominador) / denominador * 100, 2).cast("double"))


def dedup(df, chave, ordem):
    w = Window.partitionBy(chave).orderBy(ordem)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")



# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 2 — Carga e limpeza
# MAGIC
# MAGIC Todas as fontes entram aqui; daqui para baixo só usamos os DataFrames já limpos.

# COMMAND ----------

# MAGIC %md ### Etapa 6A — Inventário obrigatório da Silver (pré-carga)
# MAGIC
# MAGIC Lista as 7 fontes, mostra schema e 10 linhas de cada tabela acessível.
# MAGIC Não cria nem altera tabelas. Se faltar Silver, interrompa e valide com a equipe.

# COMMAND ----------

# ETAPA 3 — CAMINHOS DAS FONTES

STORAGE_ACCOUNT = "internshipdatalake"
host = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"

base_squad3 = f"abfss://squad3@{host}/"
base_squad2 = f"abfss://squad2@{host}/"
base_raw = f"abfss://raw@{host}/batch-data/"

silver_lojas = base_squad3 + "silver/physical_lojas"
silver_produtos = base_squad3 + "silver/physical_produtos_pereciveis"
silver_lotes = base_squad3 + "silver/food_lotes_producao"
silver_itens = base_squad3 + "silver/physical_itens_venda_caixa"
silver_estoque = base_squad3 + "silver/food_estoque_lojas"

silver_ecommerce = base_squad2 + "silver/ecommerce_produtos"
raw_vendas = base_raw + "physical_vendas_caixa.csv"

base_gold = base_squad3 + "gold/"

print("Etapa 3 carregada com sucesso!")
print("Silver lojas:", silver_lojas)

# COMMAND ----------

# ============================================================
# ETAPA 6A — INVENTÁRIO DAS FONTES DA GOLD
# ============================================================

TABELAS_SILVER = {
    "physical_lojas": silver_lojas,
    "physical_produtos_pereciveis": silver_produtos,
    "food_lotes_producao": silver_lotes,
    "physical_itens_venda_caixa": silver_itens,
    "food_estoque_lojas": silver_estoque,
    "ecommerce_produtos": silver_ecommerce,
}

inventario = []

# Verificar tabelas Silver
for nome, caminho in TABELAS_SILVER.items():
    try:
        df_inspecao = ler_delta(caminho)
        df_inspecao.limit(1).collect()

        inventario.append(
            (nome, "SILVER", "DISPONIVEL", caminho, "")
        )

        print(f"\n=== {nome} ===")
        df_inspecao.printSchema()
        display(df_inspecao.limit(10))

    except Exception as erro:
        inventario.append(
            (
                nome,
                "SILVER",
                "NAO CONFIRMADA",
                caminho,
                f"{type(erro).__name__}: {str(erro)[:180]}"
            )
        )

# Verificar vendas na Raw
try:
    df_inspecao = ler_csv(raw_vendas, schema_vendas)
    df_inspecao.limit(1).collect()

    inventario.append(
        (
            "physical_vendas_caixa",
            "RAW",
            "DISPONIVEL",
            raw_vendas,
            ""
        )
    )

    print("\n=== physical_vendas_caixa (RAW) ===")
    df_inspecao.printSchema()
    display(df_inspecao.limit(10))

except Exception as erro:
    inventario.append(
        (
            "physical_vendas_caixa",
            "RAW",
            "NAO CONFIRMADA",
            raw_vendas,
            f"{type(erro).__name__}: {str(erro)[:180]}"
        )
    )

inventario_fontes = spark.createDataFrame(
    inventario,
    """
    tabela string,
    origem string,
    status string,
    caminho string,
    detalhe string
    """
)

display(inventario_fontes)

# COMMAND ----------

# MAGIC %md ### Etapa 7 — Carga das fontes
# MAGIC
# MAGIC **O que faz**
# MAGIC - Lê lojas, produtos e lotes da **Silver** e lojas da **Bronze** (usada só na validação da Etapa 20).
# MAGIC - Lê vendas, itens, estoque e e-commerce pela função `ler_silver_ou_raw`.
# MAGIC - Imprime de onde veio cada tabela (SILVER ou RAW temporário).

# COMMAND ----------

print("Função ler_fonte_silver:",
      "ler_fonte_silver" in globals())

print("Função ler_fonte_raw:",
      "ler_fonte_raw" in globals())

print("Variável FONTES:",
      "FONTES" in globals())

# COMMAND ----------

# ============================================================
# ETAPA 7 — CARGA DAS FONTES DA GOLD
# ============================================================

# Silver — Squad 3
df_lojas = ler_fonte_silver(
    "physical_lojas", silver_lojas
)

df_produtos = ler_fonte_silver(
    "physical_produtos_pereciveis", silver_produtos
)

df_lotes = ler_fonte_silver(
    "food_lotes_producao", silver_lotes
)

df_itens_src = ler_fonte_silver(
    "physical_itens_venda_caixa", silver_itens
)

df_estoque_src = ler_fonte_silver(
    "food_estoque_lojas", silver_estoque
)

# Silver — Squad 2
df_ecommerce = ler_fonte_silver(
    "ecommerce_produtos", silver_ecommerce
)

# Raw — Vendas de caixa
df_vendas_src = ler_fonte_raw(
    "physical_vendas_caixa", raw_vendas, schema_vendas
)

# Resumo das fontes
for tabela, fonte in FONTES.items():
    print(f"{tabela:<32} -> {fonte}")

# COMMAND ----------

# MAGIC %md ### Etapa 8 — Limpeza mínima das dependências
# MAGIC
# MAGIC **O que faz**
# MAGIC - **Vendas:** remove `id_transacao` nulo/duplicado e valor ≤ 0; cria `ano`, `mes` e `semestre`.
# MAGIC - Guarda `vendas_validas` (antes do filtro de loja) e `vendas` (só lojas que existem na Silver).
# MAGIC - **Itens:** remove item nulo/duplicado, quantidade ou preço ≤ 0, e itens sem venda correspondente.
# MAGIC - **Estoque:** remove SKU/data nulos e lojas que não existem.

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 8: LIMPEZA MÍNIMA DAS DEPENDÊNCIAS

ids_lojas_validas = df_lojas.select("id_loja")

# Vendas: PK não nula e única (duplicata dobra receita), valor > 0
vendas_validas = dedup(
    df_vendas_src.filter(
        F.col("id_transacao").isNotNull()
        & F.col("id_loja").isNotNull()
        & F.col("dt_venda").isNotNull()
        & (F.col("valor_total_venda") > 0)
    ),
    "id_transacao",
    F.col("dt_venda").desc(),
).select(
    "id_transacao", "id_loja", "dt_venda", "cpf_cliente", "tipo_pagamento",
    F.col("valor_total_venda").cast("decimal(10,2)").alias("valor_total_venda"),
    F.year("dt_venda").alias("ano"),
    F.month("dt_venda").alias("mes"),
    F.when(F.month("dt_venda") <= 6, 1).otherwise(2).alias("semestre"),
)

# FK id_loja -> physical_lojas (regra técnica 2 de vendas)
vendas = vendas_validas.join(ids_lojas_validas, "id_loja", "left_semi")

# Itens: PK, quantidade > 0, preço > 0, FK id_transacao -> vendas
itens = dedup(
    df_itens_src.filter(
        F.col("id_item_venda").isNotNull()
        & F.col("codigo_barras_produto").isNotNull()
        & (F.col("quantidade") > 0)
        & (F.col("preco_unitario_registro") > 0)
    ),
    "id_item_venda",
    F.col("id_transacao"),
).join(vendas.select("id_transacao"), "id_transacao", "left_semi")

# Estoque: chaves não nulas e FK de loja
estoque = (
    df_estoque_src
        .filter(F.col("sku").isNotNull() & F.col("dt_snapshot").isNotNull())
        .withColumn("sku", F.trim("sku"))
        .join(ids_lojas_validas, "id_loja", "left_semi")
)

print("Vendas válidas:", vendas.count())
print("Itens válidos:", itens.count())
print("Registros de estoque:", estoque.count())

print("Etapa 8 concluída!")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 3 — KPIs de lojas (`physical_lojas`)
# MAGIC
# MAGIC Cada etapa gera uma tabela Gold.

# COMMAND ----------

# MAGIC %md ### Etapa 9 — Dimensão de lojas
# MAGIC
# MAGIC **O que faz**
# MAGIC - Parte do cadastro de lojas da Silver.
# MAGIC - **Com arquivo IBGE:** cruza por cidade + UF e traz população e renda; `porte_praca` = Top 10 por população.
# MAGIC
# MAGIC
# MAGIC **Regra atendida:** Lojas R7 (enriquecimento IBGE) e base da R8 (porte da praça)
# MAGIC
# MAGIC **Gera:** `gold_dim_lojas` — 1 linha por loja

# COMMAND ----------

import requests

# Top 10 municípios mais populosos - Censo IBGE 2022 (define porte_praca)
TOP10_CIDADES = [
    ("SAO PAULO", "SP"), ("RIO DE JANEIRO", "RJ"), ("BRASILIA", "DF"),
    ("FORTALEZA", "CE"), ("SALVADOR", "BA"), ("BELO HORIZONTE", "MG"),
    ("MANAUS", "AM"), ("CURITIBA", "PR"), ("RECIFE", "PE"), ("GOIANIA", "GO"),
]

# Distritos que não são municípios -> município a que pertencem (código IBGE)
DISTRITOS_PARA_MUNICIPIO = {
    "BARRA DE SAO JOAO": (3301306, "Casimiro de Abreu"),
    "CUNHAMBEBE":        (3300100, "Angra dos Reis"),
    "PORTO DAS CAIXAS":  (3301900, "Itaboraí"),
    "RIOGRANDINA":       (3303401, "Nova Friburgo"),
}

cache_ibge = base_gold + "ref_ibge_municipios"   # última consulta boa fica salva aqui


def _numero(valor):
    try:
        return float(str(valor).replace(",", "."))
    except (TypeError, ValueError):
        return None          # IBGE usa "...", "-", "X" quando não há dado


def consultar_ibge(cidades):
    """cidades: lista de (cidade_norm, uf). Retorna DataFrame com código, população e renda."""
    municipios = requests.get(
        "https://servicodados.ibge.gov.br/api/v1/localidades/municipios", timeout=60
    ).json()
    por_nome = {}
    for m in municipios:
        if m.get("regiao-imediata"):
            uf = m["regiao-imediata"]["regiao-intermediaria"]["UF"]["sigla"]
            nome = m["nome"].upper().translate(str.maketrans(_ACENTOS, _SEM_ACENTOS)).upper()
            por_nome[(nome, uf)] = (m["id"], m["nome"])

    linhas = []
    for cidade, uf in cidades:
        codigo, municipio = DISTRITOS_PARA_MUNICIPIO.get(cidade) or por_nome.get((cidade, uf), (None, None))
        populacao = renda = None

        if codigo:
            # População estimada (último período disponível)
            r = requests.get(
                "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1/"
                f"variaveis/9324?localidades=N6[{codigo}]", timeout=60
            )
            if r.ok and r.json():
                serie = r.json()[0]["resultados"][0]["series"][0]["serie"]
                populacao = _numero(serie[max(serie)])

            # Renda per capita - Censo 2022 (SIDRA)
            r = requests.get(
                f"https://apisidra.ibge.gov.br/values/t/10295/n6/{codigo}/v/13431/p/2022", timeout=60
            )
            if r.ok and len(r.json()) > 1:          # 1ª linha é cabeçalho
                renda = _numero(r.json()[1]["V"])

        linhas.append((cidade, uf, codigo, municipio,
                       int(populacao) if populacao is not None else None, renda))

    return spark.createDataFrame(
        linhas,
        "cidade_norm string, uf_norm string, codigo_ibge long, municipio_ibge string, "
        "populacao long, renda_per_capita double",
    )


lojas_base = (
    df_lojas
        .select("id_loja", "nome_loja", "cnpj", "cidade_loja", "estado_loja", "peso_vendas")
        .withColumn("cidade_norm", normalizar_texto(F.col("cidade_loja")))
        .withColumn("uf_norm", F.upper(F.trim("estado_loja")))
)

cidades = [(r["cidade_norm"], r["uf_norm"])
           for r in lojas_base.select("cidade_norm", "uf_norm").distinct().collect()]

# 1. Tenta a API do IBGE; 2. se falhar, usa a última consulta salva
try:
    ibge = consultar_ibge(cidades)
    FONTES["ibge"] = "API IBGE"

except Exception as e:
    print(
        f"Aviso: API IBGE indisponível: "
        f"{type(e).__name__}: {str(e)[:200]}"
    )

    ibge = spark.createDataFrame(
        [],
        """
        cidade_norm string,
        uf_norm string,
        codigo_ibge long,
        municipio_ibge string,
        populacao long,
        renda_per_capita double
        """
    )

    FONTES["ibge"] = "INDISPONÍVEL"

top10 = spark.createDataFrame(TOP10_CIDADES, "cidade_norm string, uf_norm string") \
    .withColumn("_top10", F.lit(True))

dim_lojas = (
    lojas_base
        .join(ibge, ["cidade_norm", "uf_norm"], "left")
        .join(top10, ["cidade_norm", "uf_norm"], "left")
        .withColumn(
            "ibge_status",
            F.when(F.col("codigo_ibge").isNull(), "municipio_nao_encontrado")
             .when(F.col("populacao").isNull() | F.col("renda_per_capita").isNull(), "dado_incompleto")
             .otherwise("ok"),
        )
        .withColumn("porte_praca",
                    F.when(F.coalesce(F.col("_top10"), F.lit(False)), "Top 10 cidades").otherwise("Demais cidades"))
        .drop("_top10", "cidade_norm", "uf_norm")
)

print("Fonte IBGE:", FONTES["ibge"])
display(dim_lojas.groupBy("ibge_status").count())
display(dim_lojas)

# COMMAND ----------

# MAGIC %md ### Etapa 10 — Visão mensal por loja
# MAGIC
# MAGIC **O que faz**
# MAGIC - Cria um calendário com todos os meses entre a primeira e a última venda.
# MAGIC - Cruza **todas as lojas × todos os meses** — meses sem venda aparecem com receita 0.
# MAGIC - Calcula receita, nº de transações, ticket médio e crescimento em relação ao mês anterior (MoM).
# MAGIC
# MAGIC **Regra atendida:** Lojas R6 (transações por mês) e R9 (granularidade 1 linha por loja por mês)
# MAGIC
# MAGIC **Gera:** `gold_lojas_mensal`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 10: VISÃO MENSAL POR LOJA

# gold_lojas_mensal  |  R6 (transações/mês) + R9 (1 linha por loja por mês)
# Inclui meses sem venda (receita 0) para manter a granularidade completa.
# ============================================================

limites = vendas.agg(
    F.min(F.trunc("dt_venda", "month")).alias("ini"),
    F.max(F.trunc("dt_venda", "month")).alias("fim"),
)

calendario = (
    limites
        .select(F.explode(F.sequence("ini", "fim", F.expr("interval 1 month"))).alias("mes_ref"))
        .select(F.year("mes_ref").alias("ano"), F.month("mes_ref").alias("mes"))
)

agg_mensal = (
    vendas.groupBy("id_loja", "ano", "mes")
        .agg(
            F.sum("valor_total_venda").alias("receita_total"),
            F.count("id_transacao").alias("qtd_transacoes"),
        )
)

zero_dec = F.lit(0).cast("decimal(20,2)")

lojas_mensal = (
    dim_lojas.select("id_loja", "nome_loja", "cidade_loja", "estado_loja", "porte_praca")
        .crossJoin(calendario)
        .join(agg_mensal, ["id_loja", "ano", "mes"], "left")
        .withColumn("receita_total", F.coalesce("receita_total", zero_dec))
        .withColumn("qtd_transacoes", F.coalesce("qtd_transacoes", F.lit(0)).cast("long"))
        .withColumn(
            "ticket_medio",
            F.when(F.col("qtd_transacoes") > 0,
                   F.round(F.col("receita_total") / F.col("qtd_transacoes"), 2))
             .cast("decimal(12,2)"),
        )
        .withColumn("_idx", F.col("ano") * 12 + F.col("mes"))
)

anterior_mes = lojas_mensal.select(
    "id_loja",
    (F.col("_idx") + 1).alias("_idx"),
    F.col("receita_total").alias("receita_mes_anterior"),
)

lojas_mensal = (
    lojas_mensal.join(anterior_mes, ["id_loja", "_idx"], "left")
        .withColumn("crescimento_mom_pct", pct(F.col("receita_total"), F.col("receita_mes_anterior")))
        .drop("_idx")
)

display(lojas_mensal.orderBy("ano", "mes", "id_loja"))


# COMMAND ----------

# MAGIC %md ### Etapa 11 — Receita anual, ranking e YoY
# MAGIC
# MAGIC **O que faz**
# MAGIC - Soma receita e transações por loja e ano.
# MAGIC - Calcula o crescimento YoY juntando cada ano com o **ano − 1** (correto mesmo se faltar um ano).
# MAGIC - Faz o ranking das lojas dentro de cada ano.
# MAGIC - `ano_completo = false` avisa quando o ano não tem os 12 meses (YoY e ranking ficam distorcidos).
# MAGIC
# MAGIC **Regra atendida:** Lojas R4 (receita/ranking anual) e R5 (crescimento YoY)
# MAGIC
# MAGIC **Gera:** `gold_lojas_receita_anual` — 1 linha por loja por ano

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 11: RECEITA ANUAL, RANKING E YOY

# gold_lojas_receita_anual  |  R4 (ranking) + R5 (YoY)
# YoY por join com ano-1 (lag() compararia anos não consecutivos
# quando uma loja não tem venda em algum ano).
# ============================================================

receita_anual = (
    vendas.groupBy("id_loja", "ano")
        .agg(
            F.sum("valor_total_venda").alias("receita_total"),
            F.count("id_transacao").alias("qtd_transacoes"),
            F.countDistinct("mes").alias("qtd_meses_com_venda"),
        )
)

anterior_ano = receita_anual.select(
    "id_loja",
    (F.col("ano") + 1).alias("ano"),
    F.col("receita_total").alias("receita_ano_anterior"),
)

lojas_receita_anual = (
    receita_anual
        .join(anterior_ano, ["id_loja", "ano"], "left")
        .withColumn("crescimento_yoy_pct", pct(F.col("receita_total"), F.col("receita_ano_anterior")))
        .withColumn(
            "ranking_receita_ano",
            F.dense_rank().over(Window.partitionBy("ano").orderBy(F.col("receita_total").desc())),
        )
        # ano incompleto distorce YoY e ranking - sinalizar para o BI
        .withColumn("ano_completo", F.col("qtd_meses_com_venda") == 12)
        .join(dim_lojas.select("id_loja", "nome_loja", "cidade_loja", "estado_loja"), "id_loja", "inner")
)

display(lojas_receita_anual.orderBy("ano", "ranking_receita_ano"))



# COMMAND ----------

# MAGIC %md ### Etapa 12 — Ticket médio por semestre
# MAGIC
# MAGIC **O que faz**
# MAGIC - Ticket médio = receita do semestre ÷ nº de transações.
# MAGIC - Traz o `porte_praca` da Etapa 9 e mostra a comparação Top 10 cidades × demais.
# MAGIC
# MAGIC **Regra atendida:** Lojas R8
# MAGIC
# MAGIC **Gera:** `gold_lojas_ticket_semestre` — 1 linha por loja por semestre

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 12: TICKET MÉDIO POR SEMESTRE

# ============================================================
# gold_lojas_ticket_semestre  |  R8
# ============================================================

lojas_ticket_semestre = (
    vendas.groupBy("id_loja", "ano", "semestre")
        .agg(
            F.sum("valor_total_venda").alias("receita_semestre"),
            F.count("id_transacao").alias("qtd_transacoes"),
        )
        .withColumn(
            "ticket_medio",
            F.round(F.col("receita_semestre") / F.col("qtd_transacoes"), 2).cast("decimal(12,2)"),
        )
        .join(
            dim_lojas.select("id_loja", "nome_loja", "cidade_loja", "estado_loja", "porte_praca"),
            "id_loja",
            "inner",
        )
)

display(lojas_ticket_semestre.orderBy("ano", "semestre", "id_loja"))

# Comparação pedida no detalhamento da regra: Top 10 cidades x demais
display(
    lojas_ticket_semestre.groupBy("ano", "semestre", "porte_praca")
        .agg(
            F.round(F.sum("receita_semestre") / F.sum("qtd_transacoes"), 2).alias("ticket_medio"),
            F.countDistinct("id_loja").alias("qtd_lojas"),
        )
        .orderBy("ano", "semestre", "porte_praca")
)


# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 4 — KPIs de perecíveis (`physical_produtos_pereciveis`)

# COMMAND ----------

# MAGIC %md ### Etapa 13 — Dimensão de produtos e catálogo unificado
# MAGIC
# MAGIC **O que faz**
# MAGIC - Monta a dimensão de perecíveis com `preco_lista` em DECIMAL(10,2).
# MAGIC - `nome_marca`: usa a coluna da Silver se existir; senão extrai do SKU e marca `nome_marca_origem = derivado_sku`.
# MAGIC - Junta perecíveis + catálogo seco (`ecommerce_produtos`) com a coluna `origem_catalogo`.
# MAGIC - Sinaliza SKUs que aparecem nos dois catálogos.
# MAGIC
# MAGIC **Regra atendida:** Perecíveis T5 (nome_marca) e R10 (distinção seco × perecível)
# MAGIC
# MAGIC **Gera:** `gold_dim_produtos_pereciveis` e `gold_catalogo_unificado`

# COMMAND ----------

# ============================================================
# ETAPA 13 — DIMENSÃO DE PRODUTOS E CATÁLOGO UNIFICADO
# ============================================================

# 1. Produtos perecíveis — Silver Squad 3

if "nome_marca" in df_produtos.columns:
    col_marca = F.col("nome_marca")
    origem_marca = F.lit("fonte")
else:
    col_marca = F.get(F.split(F.col("sku"), "-"), 1)
    origem_marca = F.lit("derivado_sku")

dim_produtos = (
    df_produtos
    .select(
        F.trim("sku").alias("sku"),
        "categoria_pai",
        "subcategoria",
        "unidade_medida",
        F.col("preco_lista").cast("decimal(10,2)").alias("preco_lista"),
        col_marca.alias("_nome_marca_fonte")
    )
    .filter(F.col("sku").isNotNull() & (F.col("sku") != ""))
    .withColumn("nome_marca", F.trim(F.col("_nome_marca_fonte")))
    .drop("_nome_marca_fonte")
    .withColumn(
        "nome_marca",
        F.when(F.col("nome_marca") != "", F.col("nome_marca"))
    )
    .withColumn("nome_marca_origem", origem_marca)
    .withColumn("origem_catalogo", F.lit("perecivel"))
)

# 2. Produtos secos — Silver Squad 2

catalogo_seco = (
    df_ecommerce
    .filter(F.col("sku").isNotNull())
    .select(
        F.trim("sku").alias("sku"),
        "id_categoria",
        "unidade_medida",
        "nome_marca",
        F.col("preco_lista").cast("decimal(10,2)").alias("preco_lista"),
        "is_ativo"
    )
    .withColumn("origem_catalogo", F.lit("seco"))
)

# 3. Unificar os catálogos preservando a origem

catalogo_unificado = (
    dim_produtos.drop("nome_marca_origem")
    .unionByName(catalogo_seco, allowMissingColumns=True)
    .withColumn(
        "_flag_sku_em_dois_catalogos",
        F.count("*").over(Window.partitionBy("sku")) > 1
    )
)

print("Etapa 13 concluída!")

print(
    "Produtos perecíveis:",
    dim_produtos.select("sku").distinct().count()
)

print(
    "Produtos secos:",
    catalogo_seco.select("sku").distinct().count()
)

print(
    "SKUs presentes em mais de um registro:",
    catalogo_unificado
        .filter(F.col("_flag_sku_em_dois_catalogos"))
        .select("sku")
        .distinct()
        .count()
)

# COMMAND ----------

# MAGIC %md ### Etapa 14 — SKUs ativos por categoria e mês
# MAGIC
# MAGIC **O que faz**
# MAGIC - Cruza os snapshots de estoque com os produtos perecíveis.
# MAGIC - `qtd_skus_ativos`: SKU apareceu em algum snapshot no mês.
# MAGIC - `qtd_skus_com_saldo`: mesmo critério, mas com quantidade > 0.
# MAGIC
# MAGIC **Regra atendida:** Perecíveis R6
# MAGIC
# MAGIC **Gera:** `gold_pereciveis_skus_ativos_mes`
# MAGIC

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 14: SKUS ATIVOS POR CATEGORIA E MÊS
######################################################################

# ============================================================
# gold_pereciveis_skus_ativos_mes  |  R6
# Não existe is_ativo no catálogo perecível. Definição adotada:
#   qtd_skus_ativos     = SKU presente em pelo menos 1 snapshot de estoque no mês
#   qtd_skus_com_saldo  = idem, com quantidade_disponivel > 0
# ============================================================

pereciveis_skus_ativos_mes = (
    estoque
        .join(dim_produtos.select("sku", "categoria_pai"), "sku", "inner")
        .withColumn("ano", F.year("dt_snapshot"))
        .withColumn("mes", F.month("dt_snapshot"))
        .groupBy("ano", "mes", "categoria_pai")
        .agg(
            F.countDistinct("sku").alias("qtd_skus_ativos"),
            F.countDistinct(
                F.when(F.col("quantidade_disponivel") > 0, F.col("sku"))
            ).alias("qtd_skus_com_saldo"),
        )
)

display(pereciveis_skus_ativos_mes.orderBy("ano", "mes", "categoria_pai"))



# COMMAND ----------

# MAGIC %md ### Etapa 15 — Preço médio por subcategoria e semestre
# MAGIC
# MAGIC **O que faz**
# MAGIC - O catálogo tem só o preço atual, sem data — então usamos o **preço registrado no caixa** (itens × vendas).
# MAGIC - Calcula preço médio, preço ponderado pela quantidade e o preço de lista como referência.
# MAGIC - Calcula a variação em relação ao semestre anterior.
# MAGIC
# MAGIC **Regra atendida:** Perecíveis R7
# MAGIC
# MAGIC **Gera:** `gold_pereciveis_preco_semestre`

# COMMAND ----------

# ============================================================
# ETAPA 15 — PREÇO MÉDIO POR SUBCATEGORIA E SEMESTRE
# ============================================================

from pyspark.sql import functions as F

# 1. Dimensão de produtos perecíveis

produtos_preco = (
    dim_produtos
    .select(
        F.col("sku").alias("codigo_barras_produto"),
        "categoria_pai",
        "subcategoria",
        "preco_lista"
    )
    .dropDuplicates(["codigo_barras_produto"])
)

# 2. Relacionar itens, vendas e produtos

base_preco = (
    itens
    .drop("ano", "semestre")
    .join(
        vendas.select(
            "id_transacao",
            "ano",
            "semestre"
        ),
        "id_transacao",
        "inner"
    )
    .join(
        produtos_preco,
        "codigo_barras_produto",
        "inner"
    )
)

# 3. Calcular preços por semestre

preco_semestre = (
    base_preco
    .groupBy(
        "ano",
        "semestre",
        "categoria_pai",
        "subcategoria"
    )
    .agg(
        F.round(
            F.avg("preco_unitario_registro"), 2
        ).alias("preco_medio_registrado"),

        F.round(
            F.sum("valor_total_item") / F.sum("quantidade"), 2
        ).alias("preco_medio_ponderado"),

        F.round(
            F.avg("preco_lista"), 2
        ).alias("preco_lista_medio_catalogo"),

        F.countDistinct(
            "codigo_barras_produto"
        ).alias("qtd_skus_vendidos"),

        F.count("*").alias("qtd_itens")
    )
    .withColumn(
        "_idx",
        F.col("ano") * 2 + F.col("semestre")
    )
)

# 4. Preço ponderado do semestre anterior

anterior_sem = (
    preco_semestre
    .select(
        "categoria_pai",
        "subcategoria",
        (F.col("_idx") + 1).alias("_idx"),
        F.col("preco_medio_ponderado").alias(
            "preco_medio_semestre_anterior"
        )
    )
)

# 5. Calcular variação percentual

pereciveis_preco_semestre = (
    preco_semestre
    .join(
        anterior_sem,
        ["categoria_pai", "subcategoria", "_idx"],
        "left"
    )
    .withColumn(
        "variacao_preco_pct",
        pct(
            F.col("preco_medio_ponderado"),
            F.col("preco_medio_semestre_anterior")
        )
    )
    .drop("_idx")
)

# 6. Exibir resultado

print("Etapa 15 concluída!")

display(
    pereciveis_preco_semestre.orderBy(
        "categoria_pai",
        "subcategoria",
        "ano",
        "semestre"
    )
)

# COMMAND ----------

# MAGIC %md ### Etapa 16 — Validade média por categoria
# MAGIC
# MAGIC **O que faz**
# MAGIC - Usa os lotes da Silver, descartando os marcados com `_flag_validade_invertida`.
# MAGIC - Dias de validade = `dt_validade − dt_fabricacao` (só valores > 0).
# MAGIC - Calcula média, mediana, mínimo, máximo e o % de lotes fora da janela esperada (Padaria 1–3, Hortifruti 3–10, Congelados 120–365 dias).
# MAGIC
# MAGIC **Regra atendida:** Perecíveis R8
# MAGIC
# MAGIC **Gera:** `gold_pereciveis_validade_categoria`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 16: VALIDADE MÉDIA POR CATEGORIA

FAIXAS_VALIDADE = [
    ("Padaria Fresca", 1, 3),
    ("Hortifruti", 3, 10),
    ("Congelados", 120, 365),
]
faixas = spark.createDataFrame(
    FAIXAS_VALIDADE,
    "categoria_pai string, validade_min_esperada int, validade_max_esperada int",
)

lotes_ok = df_lotes.filter(
    F.col("dt_fabricacao").isNotNull() & F.col("dt_validade").isNotNull()
)
if "_flag_validade_invertida" in df_lotes.columns:
    lotes_ok = lotes_ok.filter(~F.coalesce(F.col("_flag_validade_invertida"), F.lit(False)))

lotes_validade = (
    lotes_ok
        .withColumn("dias_validade", F.datediff("dt_validade", "dt_fabricacao"))
        .filter(F.col("dias_validade") > 0)   # regra: validade POSTERIOR à fabricação
        .join(
    dim_produtos
        .select("sku", "categoria_pai")
        .dropDuplicates(["sku", "categoria_pai"]),
    "sku",
    "inner"
)
        .join(faixas, "categoria_pai", "left")
        .withColumn(
            "_fora_faixa",
            F.when(F.col("validade_min_esperada").isNull(), F.lit(None))
             .when(
                 (F.col("dias_validade") < F.col("validade_min_esperada"))
                 | (F.col("dias_validade") > F.col("validade_max_esperada")), 1
             ).otherwise(0),
        )
)

pereciveis_validade_categoria = (
    lotes_validade.groupBy("categoria_pai", "validade_min_esperada", "validade_max_esperada")
        .agg(
            F.round(F.avg("dias_validade"), 1).alias("validade_media_dias"),
            F.percentile_approx("dias_validade", 0.5).alias("validade_mediana_dias"),
            F.min("dias_validade").alias("validade_min_dias"),
            F.max("dias_validade").alias("validade_max_dias"),
            F.count("*").alias("qtd_lotes"),
            F.round(F.avg("_fora_faixa") * 100, 2).alias("pct_lotes_fora_faixa"),
        )
)

display(pereciveis_validade_categoria.orderBy("categoria_pai"))


# COMMAND ----------

# MAGIC %md ### Etapa 17 — Validação dos SKUs de itens e estoque
# MAGIC
# MAGIC **O que faz**
# MAGIC - Pega todos os identificadores de produto de itens de venda e de estoque.
# MAGIC - Classifica cada um como `perecivel`, `seco`, `orfao` (não está em nenhum catálogo) ou `conflito_dois_catalogos`.
# MAGIC - Mostra um resumo por tabela de origem.
# MAGIC
# MAGIC **Regra atendida:** Perecíveis R9
# MAGIC
# MAGIC **Gera:** `gold_pereciveis_validacao_sku`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 17: VALIDAÇÃO DOS SKUS DE ITENS E ESTOQUE

# gold_pereciveis_validacao_sku  |  R9
# Classifica cada identificador de itens e estoque como perecível,
# seco (ecommerce_produtos) ou órfão. Substitui o %sql que falhou
# (as tabelas não estavam registradas como views).
# ============================================================

ids_origem = (
    df_itens_src.select(F.trim("codigo_barras_produto").alias("sku"))
        .withColumn("tabela_origem", F.lit("physical_itens_venda_caixa"))
        .unionByName(
            df_estoque_src.select(F.trim("sku").alias("sku"))
                .withColumn("tabela_origem", F.lit("food_estoque_lojas"))
        )
        .filter(F.col("sku").isNotNull())
        .distinct()
)

skus_pereciveis = (
    dim_produtos
    .select(F.trim("sku").alias("sku"))
    .filter(F.col("sku").isNotNull() & (F.col("sku") != ""))
    .distinct()
    .withColumn("_perecivel", F.lit(True))
)

skus_secos = (
    catalogo_seco
    .select(F.trim("sku").alias("sku"))
    .filter(F.col("sku").isNotNull() & (F.col("sku") != ""))
    .distinct()
    .withColumn("_seco", F.lit(True))
)

pereciveis_validacao_sku = (
    ids_origem
        .join(skus_pereciveis, "sku", "left")
        .join(skus_secos, "sku", "left")
        .withColumn(
            "origem_catalogo",
            F.when(F.col("_perecivel") & F.col("_seco"), "conflito_dois_catalogos")
             .when(F.col("_perecivel"), "perecivel")
             .when(F.col("_seco"), "seco")
             .otherwise("orfao"),
        )
        .drop("_perecivel", "_seco")
)

resumo_sku = (
    pereciveis_validacao_sku.groupBy("tabela_origem", "origem_catalogo")
        .agg(F.count("*").alias("qtd_identificadores"))
        .orderBy("tabela_origem", "origem_catalogo")
)
display(resumo_sku)


# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 5 — Documentação e gravação

# COMMAND ----------

# MAGIC %md ### Etapa 18 — Dicionário de dados
# MAGIC
# MAGIC **O que faz**
# MAGIC - Cria uma tabela com nome, granularidade, descrição e chave de cada tabela Gold.
# MAGIC - Registra a granularidade de lojas (1 linha por loja por mês) e a diferença entre catálogo seco e perecível.
# MAGIC
# MAGIC **Regra atendida:** Lojas R9 e Perecíveis R10
# MAGIC
# MAGIC **Gera:** `gold_dicionario_dados`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 18: DICIONÁRIO DE DADOS
######################################################################

DICIONARIO = [
    (
        "gold_dim_lojas",
        "1 linha por loja",
        "Cadastro de lojas válidas (Silver) + porte_praca e dados IBGE "
        "(populacao, renda_per_capita).",
        "id_loja"
    ),
    (
        "gold_lojas_mensal",
        "1 linha por loja por mês",
        "Receita, número de transações, ticket médio e crescimento MoM. "
        "Meses sem venda aparecem com 0.",
        "id_loja, ano, mes"
    ),
    (
        "gold_lojas_receita_anual",
        "1 linha por loja por ano",
        "Receita anual, ranking no ano e crescimento YoY. "
        "ano_completo=false indica ano parcial.",
        "id_loja, ano"
    ),
    (
        "gold_lojas_ticket_semestre",
        "1 linha por loja por semestre",
        "Ticket médio = receita / número de transações, "
        "com porte_praca (Top 10 cidades x demais).",
        "id_loja, ano, semestre"
    ),
    (
        "gold_lojas_validacao",
        "1 linha por id_loja",
        "Validação da presença da loja nas fontes disponíveis "
        "e nas vendas, com status para explicar ausências. "
        "Depende de confirmação da etapa de validação de lojas.",
        "id_loja"
    ),
    (
        "gold_dim_produtos_pereciveis",
        "1 linha por SKU perecível",
        "Catálogo PERECÍVEL da loja física. "
        "nome_marca_origem='derivado_sku' indica marca extraída do SKU.",
        "sku"
    ),
    (
        "gold_catalogo_unificado",
        "1 linha por SKU por origem_catalogo",
        "União dos catálogos SECO (Squad 2) e PERECÍVEL (Squad 3). "
        "Um SKU pode aparecer nos dois catálogos. "
        "Considerar origem_catalogo nos relacionamentos.",
        "sku, origem_catalogo"
    ),
    (
        "gold_pereciveis_skus_ativos_mes",
        "1 linha por categoria_pai por mês",
        "SKU ativo = aparece em snapshot de estoque no mês. "
        "qtd_skus_com_saldo exige quantidade > 0.",
        "ano, mes, categoria_pai"
    ),
    (
        "gold_pereciveis_preco_semestre",
        "1 linha por subcategoria por semestre",
        "Preço médio simples, preço médio ponderado pela quantidade vendida "
        "e variação percentual do preço ponderado em relação "
        "ao semestre anterior.",
        "ano, semestre, categoria_pai, subcategoria"
    ),
    (
        "gold_pereciveis_validade_categoria",
        "1 linha por categoria_pai",
        "Validade média e mediana em dias dos lotes "
        "e percentual fora da janela esperada.",
        "categoria_pai"
    ),
    (
        "gold_pereciveis_validacao_sku",
        "1 linha por identificador por tabela de origem",
        "Classifica SKUs de itens de venda e estoque em "
        "perecivel / seco / orfao / conflito.",
        "tabela_origem, sku"
    )
]

# Criar DataFrame do dicionário

dicionario = spark.createDataFrame(
    DICIONARIO,
    "tabela string, granularidade string, descricao string, chave string"
)

# Exibir dicionário

display(dicionario)

# COMMAND ----------

# MAGIC %md ### Etapa 18A — Prévia e validação das tabelas Gold (sem escrita)
# MAGIC
# MAGIC Confira as amostras e as chaves antes de ativar a gravação.

# COMMAND ----------

TABELAS_GOLD = [
    ("gold_dim_lojas", dim_lojas, ["id_loja"]),
    ("gold_lojas_mensal", lojas_mensal, ["id_loja", "ano", "mes"]),
    ("gold_lojas_receita_anual", lojas_receita_anual, ["id_loja", "ano"]),
    ("gold_lojas_ticket_semestre", lojas_ticket_semestre, ["id_loja", "ano", "semestre"]),
    ("gold_dim_produtos_pereciveis", dim_produtos, ["sku"]),
    ("gold_catalogo_unificado", catalogo_unificado, ["sku", "origem_catalogo"]),
    ("gold_pereciveis_skus_ativos_mes", pereciveis_skus_ativos_mes, ["ano", "mes", "categoria_pai"]),
    ("gold_pereciveis_preco_semestre", pereciveis_preco_semestre, ["ano", "semestre", "categoria_pai", "subcategoria"]),
    ("gold_pereciveis_validade_categoria", pereciveis_validade_categoria, ["categoria_pai"]),
    ("gold_pereciveis_validacao_sku", pereciveis_validacao_sku, ["tabela_origem", "sku"]),
    ("gold_dicionario_dados", dicionario, ["tabela"]),
]

resumo_previa = []
for nome, df, chaves in TABELAS_GOLD:
    total = df.count()
    distintas = df.select(*chaves).distinct().count()
    nulas = df.filter(F.expr(" OR ".join(f"`{c}` IS NULL" for c in chaves))).count()
    resumo_previa.append((nome, total, distintas, total - distintas, nulas))
    print(f"\n=== {nome} | chaves: {', '.join(chaves)} ===")
    display(df.limit(10))

resumo_previa_gold = spark.createDataFrame(
    resumo_previa,
    "tabela string, linhas long, chaves_distintas long, duplicadas long, chaves_nulas long"
)
display(resumo_previa_gold)

if resumo_previa_gold.filter("duplicadas > 0 OR chaves_nulas > 0").count() > 0:
    raise ValueError("Gold possui chaves duplicadas ou nulas. Corrija antes do MERGE.")

# COMMAND ----------

# MAGIC %md ### Etapa 19 — Gravação das tabelas Gold (Delta + SQL Server)
# MAGIC
# MAGIC **O que faz**
# MAGIC - Cada linha grava uma tabela com **MERGE pela sua chave** (ex.: `gold_lojas_mensal` por `id_loja, ano, mes`) na pasta `gold/` e no SQL Server.
# MAGIC - Imprime, para cada tabela, como foi gravada: `criacao`, `merge` ou `upsert_overwrite` no Delta, e `criacao` / `merge` / `ERRO` no SQL Server.
# MAGIC - Se alguma tabela não chegar ao SQL Server, avisa no final.

# COMMAND ----------

# Controle de segurança da gravação Gold
EXECUTAR_GRAVACAO = False

# SQL Server permanece desabilitado
ENVIAR_SQL_SERVER = False

# COMMAND ----------

# ============================================================
# ETAPA 19 — VALIDAÇÃO DAS TABELAS GOLD
# Somente leitura: não grava nem altera dados no Azure
# ============================================================

from pyspark.sql import functions as F

TABELAS_GOLD = [
    ("gold_dim_lojas", dim_lojas, ["id_loja"]),

    ("gold_lojas_mensal", lojas_mensal,
     ["id_loja", "ano", "mes"]),

    ("gold_lojas_receita_anual", lojas_receita_anual,
     ["id_loja", "ano"]),

    ("gold_lojas_ticket_semestre", lojas_ticket_semestre,
     ["id_loja", "ano", "semestre"]),

    ("gold_dim_produtos_pereciveis", dim_produtos,
     ["sku"]),

    ("gold_catalogo_unificado", catalogo_unificado,
     ["sku", "origem_catalogo"]),

    ("gold_pereciveis_skus_ativos_mes", pereciveis_skus_ativos_mes,
     ["ano", "mes", "categoria_pai"]),

    ("gold_pereciveis_preco_semestre", pereciveis_preco_semestre,
     ["ano", "semestre", "categoria_pai", "subcategoria"]),

    ("gold_pereciveis_validade_categoria", pereciveis_validade_categoria,
     ["categoria_pai"]),

    ("gold_pereciveis_validacao_sku", pereciveis_validacao_sku,
     ["tabela_origem", "sku"]),

    ("gold_dicionario_dados", dicionario,
     ["tabela"])
]

resumo_previa = []

for nome, df, chaves in TABELAS_GOLD:

    total = df.count()

    distintas = df.select(*chaves).distinct().count()

    duplicadas = total - distintas

    condicao_nulos = " OR ".join(
        f"`{chave}` IS NULL" for chave in chaves
    )

    nulas = df.filter(F.expr(condicao_nulos)).count()

    resumo_previa.append(
        (nome, total, distintas, duplicadas, nulas)
    )

    print(
        f"{nome} | Linhas: {total} | "
        f"Duplicadas: {duplicadas} | "
        f"Chaves nulas: {nulas}"
    )

resumo_previa_gold = spark.createDataFrame(
    resumo_previa,
    """
    tabela STRING,
    linhas LONG,
    chaves_distintas LONG,
    duplicadas LONG,
    chaves_nulas LONG
    """
)

print("\n========== RESUMO DA VALIDAÇÃO GOLD ==========")

display(resumo_previa_gold)

problemas = resumo_previa_gold.filter(
    (F.col("duplicadas") > 0) |
    (F.col("chaves_nulas") > 0)
).count()

if problemas > 0:
    print(
        f"ATENÇÃO: {problemas} tabela(s) apresentam "
        "duplicidades ou chaves nulas."
    )
else:
    print(
        "VALIDAÇÃO APROVADA: todas as 11 tabelas "
        "estão sem chaves duplicadas ou nulas."
    )

# COMMAND ----------

# ============================================================
# ETAPA 19 — GRAVAÇÃO DEFINITIVA DAS TABELAS GOLD
# SQUAD 3 | DELTA LAKE | SQL SERVER DESABILITADO
# ============================================================

# Confirma que a validação foi aprovada
if problemas > 0:
    raise RuntimeError(
        "Gravação bloqueada: existem problemas nas chaves."
    )

# Habilita gravação
EXECUTAR_GRAVACAO = True
ENVIAR_SQL_SERVER = False

# Limpa o resumo de execuções anteriores
RESUMO_GRAVACAO.clear()

# Grava as 11 tabelas Gold
for nome, df, chaves in TABELAS_GOLD:

    particoes = ["ano"] if nome == "gold_lojas_mensal" else None

    salvar_gold(
        df=df,
        nome=nome,
        chaves=chaves,
        particoes=particoes
    )

# Bloqueia novamente a gravação
EXECUTAR_GRAVACAO = False

print("\n========== RESUMO DA GRAVAÇÃO ==========")

for registro in RESUMO_GRAVACAO:
    print(registro)

print("\nProcessamento Gold finalizado!")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 6 — Validações pós-gravação
# MAGIC
# MAGIC Lê de volta o que foi gravado e confere.

# COMMAND ----------

# MAGIC %md ### Etapa 20 — Validações finais
# MAGIC
# MAGIC **O que faz**
# MAGIC - Compara as lojas da Bronze, Silver, Gold e das vendas e dá um `status` para cada uma: `ok`, `sem_vendas`, `rejeitada_na_silver`, `venda_sem_cadastro`…
# MAGIC - Confere se `gold_lojas_mensal` tem exatamente lojas × meses linhas, sem duplicatas.
# MAGIC - Conta SKUs órfãos ou em conflito (Etapa 17).
# MAGIC - Se `FALHAR_SE_INCONSISTENTE = True` e houver problema, interrompe o notebook.
# MAGIC
# MAGIC **Regra atendida:** Lojas R10
# MAGIC
# MAGIC **Gera:** `gold_lojas_validacao` e, no fim, o resumo de onde cada tabela foi gravada (Delta e SQL Server)

# COMMAND ----------

# ETAPA 20 — VALIDAÇÃO DA GOLD PERSISTIDA

for nome, df, chaves in TABELAS_GOLD:
    caminho = base_gold + nome

    df_gravado = ler_delta(caminho)
    total_gravado = df_gravado.count()
    total_esperado = df.count()

    status = "OK" if total_gravado == total_esperado else "DIVERGENTE"

    print(
        f"{nome} | "
        f"Esperado: {total_esperado} | "
        f"Gravado: {total_gravado} | "
        f"Status: {status}"
    )