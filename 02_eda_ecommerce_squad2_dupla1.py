# Databricks notebook source
# MAGIC %md
# MAGIC ## 🧾 Ficha de identificação
# MAGIC
# MAGIC | Informação | Detalhe |
# MAGIC |---|---|
# MAGIC | **Notebook** | `02_eda_ecommerce_squad2_dupla1` |
# MAGIC | **Camada** | Análise exploratória (somente leitura — não grava nada) |
# MAGIC | **Tabelas / saídas principais** | Nenhuma tabela própria; imprime diagnósticos de Bronze, Silver e quarentena |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | `bronze/ecommerce_*`, `silver/ecommerce_*` e `silver/rejeitados_ecommerce_*` (Delta, via `00_config`) |
# MAGIC | **Destino** | — (só leitura) |
# MAGIC | **Motivo** | Conhecer o dado (volumetria, versões, nulos, preços, margem, integridade, quarentena) e embasar as regras da Silver e os KPIs da Gold |
# MAGIC | **Última atualização** | 22/10/2026 |
# MAGIC | **Observações** | Pode ser executado a qualquer momento depois do `03` e do `04`; não altera estado do pipeline |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 20/09/2026 | Lucas Sousa | Criação do EDA sobre `ecommerce_produtos` e `ecommerce_categorias` |
# MAGIC | 22/10/2026 | Lucas Sousa | Notebook recriado após exclusão acidental e reescrito: lê Bronze/Silver/quarentena Delta via `00_config` (o `01` e o pandas saíram da arquitetura); inclui versões por SKU, `is_ativo`, regras da Silver aplicadas ao dado cru, `preco_custo`/margem e quarentena |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 · Análise exploratória (EDA)
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC | Seção | Pergunta que responde |
# MAGIC |---|---|
# MAGIC | 1 | Quantas linhas e chaves há em cada camada? |
# MAGIC | 2 | O Bronze guarda várias versões do mesmo SKU? Os preços mudam entre janelas? |
# MAGIC | 3 | Qual o schema da Silver? |
# MAGIC | 4 | Quanto é nulo, por coluna? |
# MAGIC | 5 | Como se distribuem preço e custo? |
# MAGIC | 6 | Como está `is_ativo`? |
# MAGIC | 7 | Quanto do dado cru viola as regras da Silver (SKU, preço, `is_ativo`)? |
# MAGIC | 8 | Custo e margem fazem sentido? |
# MAGIC | 9 | Produtos e categorias se relacionam corretamente? |
# MAGIC | 10 | O que está indo para a quarentena e por quê? |
# MAGIC
# MAGIC Os limites das regras (SKU entre 5 e 60 caracteres, preço entre 0 e 5.000) repetem a planilha oficial usada em `04`; se a planilha mudar, ajuste aqui também.

# COMMAND ----------

# MAGIC %run ./00_config_squad2_dupla1

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame, functions as F

ausentes = [
    nome for nome, caminho in {
        "bronze/ecommerce_produtos": CAMINHO_BRONZE_PRODUTOS,
        "bronze/ecommerce_categorias": CAMINHO_BRONZE_CATEGORIAS,
        "silver/ecommerce_produtos": CAMINHO_SILVER_PRODUTOS,
        "silver/ecommerce_categorias": CAMINHO_SILVER_CATEGORIAS,
    }.items() if not tabela_delta_existe(caminho)
]
if ausentes:
    raise RuntimeError(f"Tabelas ausentes: {ausentes} — rode os notebooks 03 e 04 antes do EDA.")

# Sem .cache()/.persist(): o Serverless não suporta. Cada ação relê o Delta; o volume (milhares de linhas) comporta.
bronze_produtos = ler_delta(CAMINHO_BRONZE_PRODUTOS)
bronze_categorias = ler_delta(CAMINHO_BRONZE_CATEGORIAS)
silver_produtos = ler_delta(CAMINHO_SILVER_PRODUTOS)
silver_categorias = ler_delta(CAMINHO_SILVER_CATEGORIAS)
rejeitados_produtos = (
    ler_delta(CAMINHO_SILVER_REJEITADOS_PRODUTOS) if tabela_delta_existe(CAMINHO_SILVER_REJEITADOS_PRODUTOS) else None
)
rejeitados_categorias = (
    ler_delta(CAMINHO_SILVER_REJEITADOS_CATEGORIAS) if tabela_delta_existe(CAMINHO_SILVER_REJEITADOS_CATEGORIAS) else None
)
TEM_PRECO_CUSTO = "preco_custo" in silver_produtos.columns


def titulo(texto: str) -> None:
    print(f"\n=== {texto} ===")


print(f"EDA pronta | preco_custo na Silver: {TEM_PRECO_CUSTO} | quarentena de produtos: {rejeitados_produtos is not None}")

# COMMAND ----------

# MAGIC %md ## 1. Volumetria por camada

# COMMAND ----------

def volumetria(nome: str, df: DataFrame, chave: str | None = None) -> None:
    total = df.count()
    linha = f"{nome:<34} linhas={total:>8}"
    if chave and chave in df.columns:
        distintas = df.select(chave).distinct().count()
        linha += f"  {chave} distintos={distintas:>8}  excedentes={total - distintas:>8}"
    print(linha)


titulo("Volumetria")
volumetria("Bronze produtos (todas as versões)", bronze_produtos, "sku")
volumetria("Silver produtos (versão vigente)", silver_produtos, "sku")      # esperado: excedentes = 0
volumetria("Bronze categorias", bronze_categorias, "id_categoria")
volumetria("Silver categorias", silver_categorias, "id_categoria")           # esperado: excedentes = 0
if rejeitados_produtos is not None:
    volumetria("Quarentena produtos", rejeitados_produtos, "sku")
if rejeitados_categorias is not None:
    volumetria("Quarentena categorias", rejeitados_categorias, "id_categoria")

# COMMAND ----------

# MAGIC %md ## 2. Bronze — versões por SKU e variação de preço
# MAGIC
# MAGIC O Bronze acumula um arquivo por janela (`AAAA/MM/DD/HHMMSS`). Um SKU pode aparecer em várias janelas; a Silver fica com a mais recente.

# COMMAND ----------

b = bronze_produtos.withColumn("janela", F.regexp_extract("arquivo_origem", PADRAO_JANELA, 1))

titulo("Janelas de origem no Bronze")
b.agg(
    F.countDistinct("janela").alias("janelas"), F.min("janela").alias("primeira"), F.max("janela").alias("ultima"),
    F.min("data_ingestao").alias("primeira_ingestao"), F.max("data_ingestao").alias("ultima_ingestao"),
).show(truncate=False)

por_sku = b.where(F.col("sku").isNotNull()).groupBy("sku").agg(
    F.countDistinct("janela").alias("n_janelas"),
    F.countDistinct("preco_lista").alias("n_precos"),
)

titulo("SKUs por número de janelas em que aparecem")
por_sku.groupBy("n_janelas").count().orderBy("n_janelas").show(20)

titulo("SKUs cujo preço mudou entre janelas")
total_skus_bronze = por_sku.count()
mudaram = por_sku.where(F.col("n_precos") > 1).count()
pct = (100 * mudaram / total_skus_bronze) if total_skus_bronze else 0
print(f"{mudaram} de {total_skus_bronze} SKUs ({pct:.2f}%) tiveram mais de um preço_lista no Bronze")

# COMMAND ----------

# MAGIC %md ## 3. Schema da Silver

# COMMAND ----------

titulo("Schema: silver/ecommerce_produtos")
silver_produtos.printSchema()
titulo("Schema: silver/ecommerce_categorias")
silver_categorias.printSchema()

# COMMAND ----------

# MAGIC %md ## 4. Nulos por coluna (Silver)

# COMMAND ----------

def analisar_nulos(df: DataFrame, nome: str) -> None:
    total = df.count()
    titulo(f"Nulos: {nome} ({total} linhas)")
    # 1 agregação para todas as colunas, em vez de 1 job por coluna
    nulos = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    for coluna, qtd in nulos.items():
        qtd = qtd or 0
        print(f"  {coluna:<32}{qtd:>8}  {(100 * qtd / total) if total else 0:6.2f}%")


analisar_nulos(silver_produtos, "silver/ecommerce_produtos")
analisar_nulos(silver_categorias, "silver/ecommerce_categorias")

# COMMAND ----------

# MAGIC %md ## 5. Preço e custo — distribuição (Silver)

# COMMAND ----------

numericas = [F.col("preco_lista").cast("double").alias("preco_lista")]
if TEM_PRECO_CUSTO:
    numericas.append(F.col("preco_custo").cast("double").alias("preco_custo"))
precos = silver_produtos.select(*numericas)

titulo("Estatísticas descritivas")
precos.summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show(truncate=False)

titulo("Cauda da distribuição (P90 / P95 / P99)")
precos.select(*[F.percentile_approx(c, [0.9, 0.95, 0.99]).alias(f"{c}_p90_p95_p99") for c in precos.columns]).show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 6. `is_ativo` — distribuição (Silver)
# MAGIC
# MAGIC Tudo `true` aqui não é erro de cálculo da Gold: é o que a fonte entrega (investigação de 15/10/2026). Segue pendente com o gestor se isso é esperado.

# COMMAND ----------

titulo("Distribuição de is_ativo")
silver_produtos.groupBy("is_ativo").count().orderBy("is_ativo").show()

# COMMAND ----------

# MAGIC %md ## 7. Dado cru (Bronze) x regras da Silver
# MAGIC
# MAGIC Quanto do Bronze seria rejeitado pelas regras 1 a 3 (linha a linha, antes de escolher a versão vigente).

# COMMAND ----------

titulo("Violações por regra no Bronze (linhas, não SKUs)")
bronze_produtos.agg(
    F.count("*").alias("linhas"),
    F.sum(F.col("sku").isNull().cast("int")).alias("regra1_sku_nulo"),
    F.sum(((F.length("sku") <= 5) | (F.length("sku") >= 60)).cast("int")).alias("regra1_tamanho_fora"),
    F.sum(F.col("preco_lista").isNull().cast("int")).alias("regra2_preco_nulo"),
    F.sum((F.col("preco_lista").cast("double") <= 0).cast("int")).alias("regra2_preco_menor_igual_0"),
    F.sum((F.col("preco_lista").cast("double") >= 5000).cast("int")).alias("regra2_preco_maior_igual_5000"),
    F.sum(F.col("is_ativo").isNull().cast("int")).alias("regra3_is_ativo_nulo"),
).show(truncate=False)

titulo("Tamanho do SKU (Bronze)")
bronze_produtos.select(F.length("sku").alias("tamanho_sku")).agg(
    F.min("tamanho_sku").alias("min"), F.expr("percentile_approx(tamanho_sku, 0.5)").alias("mediana"), F.max("tamanho_sku").alias("max"),
).show()

# COMMAND ----------

# MAGIC %md ## 8. Custo e margem (Silver)

# COMMAND ----------

if not TEM_PRECO_CUSTO:
    print("preco_custo ainda não existe na Silver — seção não se aplica.")
else:
    titulo("Cobertura de custo e margem")
    silver_produtos.select(
        F.col("preco_lista").cast("double").alias("p"), F.col("preco_custo").cast("double").alias("c"), "is_ativo",
    ).agg(
        F.count("*").alias("skus"),
        F.sum(F.col("c").isNotNull().cast("int")).alias("com_custo"),
        F.sum((F.col("c") >= F.col("p")).cast("int")).alias("custo_maior_ou_igual_ao_preco"),
        F.avg(F.when(F.col("p") > 0, (F.col("p") - F.col("c")) / F.col("p"))).alias("margem_media_simples_sobre_preco"),
        F.expr("percentile_approx(CASE WHEN p > 0 THEN (p - c) / p END, 0.5)").alias("margem_mediana"),
    ).show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 9. Integridade referencial e hierarquia de categorias

# COMMAND ----------

titulo("Produtos órfãos (id_categoria sem correspondência na Silver de categorias)")
orfaos = silver_produtos.join(silver_categorias.select("id_categoria"), "id_categoria", "left_anti")
n_orfaos = orfaos.count()
print(f"Produtos órfãos: {n_orfaos}")
if n_orfaos:
    orfaos.select("id_categoria").distinct().orderBy("id_categoria").show(10, truncate=False)   # orderBy: amostra estável entre execuções

titulo("Categorias")
sem_produto = silver_categorias.join(silver_produtos.select("id_categoria").distinct(), "id_categoria", "left_anti").count()
raizes = silver_categorias.where(F.col("id_categoria_pai").isNull()).count()
print(f"Categorias: {silver_categorias.count()} | raiz (sem pai): {raizes} | sem nenhum produto: {sem_produto}")

titulo("Top 10 categorias por número de SKUs")
(
    silver_produtos.groupBy("id_categoria").count()
    .join(silver_categorias.select("id_categoria", "nome_categoria"), "id_categoria", "left")
    .orderBy(F.col("count").desc(), "id_categoria")
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md ## 10. Quarentena de produtos

# COMMAND ----------

if rejeitados_produtos is None:
    print("Sem quarentena de produtos — seção não se aplica.")
else:
    q = rejeitados_produtos
    if "motivo_rejeicao" not in q.columns:
        q = q.withColumn("motivo_rejeicao", F.lit(None).cast("string"))
    motivo = F.when(F.col("motivo_rejeicao").isNull() | (F.col("motivo_rejeicao") == ""), F.lit("motivo_nao_registrado")).otherwise(F.col("motivo_rejeicao"))

    titulo("Rejeições por regra (ocorrências x SKUs distintos)")
    (
        q.withColumn("regra", F.explode(F.split(motivo, ",")))
        .groupBy("regra").agg(F.count("*").alias("ocorrencias"), F.countDistinct("sku").alias("skus"))
        .orderBy(F.col("ocorrencias").desc())
        .show(truncate=False)
    )

    titulo("SKUs rejeitados: ainda têm versão válida na Silver?")
    skus_rejeitados = q.select("sku").where(F.col("sku").isNotNull()).distinct()
    total_rej = skus_rejeitados.count()
    com_versao = skus_rejeitados.join(silver_produtos.select("sku").distinct(), "sku", "left_semi").count()
    print(f"SKUs distintos na quarentena: {total_rej} | com versão válida (preço antigo preservado): {com_versao} | fora do catálogo: {total_rej - com_versao}")

    titulo("Rejeições por dia")
    q.groupBy(F.to_date("data_rejeicao").alias("dia")).count().orderBy("dia").show(40)

    titulo("Preço rejeitado (preco_lista) — faixas")
    q.select(F.col("preco_lista").cast("double").alias("p")).agg(
        F.sum((F.col("p") <= 0).cast("int")).alias("zero_ou_negativo"),
        F.sum((F.col("p") >= 5000).cast("int")).alias("maior_ou_igual_5000"),
        F.sum(F.col("p").isNull().cast("int")).alias("nulo"),
    ).show()

# COMMAND ----------

# MAGIC %md ## Encerramento

# COMMAND ----------

print("EDA concluída (somente leitura — nenhuma tabela foi alterada).")
