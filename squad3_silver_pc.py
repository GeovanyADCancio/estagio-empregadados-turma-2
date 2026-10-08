# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md # Silver — Squad 3 
# MAGIC
# MAGIC Transforma a Bronze de **`physical_lojas`** e **`physical_produtos_pereciveis`** aplicando as regras técnicas.
# MAGIC Registros que não passam vão para uma tabela de **quarentena com o motivo** — nada some sem rastro.
# MAGIC
# MAGIC | Parte | Etapas | O que acontece |
# MAGIC |---|---|---|
# MAGIC | 1. Preparação | 1 – 5 | Imports, credenciais, caminhos e funções reutilizáveis |
# MAGIC | 2. Lojas | 6 – 7 | Limpa, valida, separa válidos × rejeitados e grava |
# MAGIC | 3. Produtos perecíveis | 8 – 9 | Limpa, valida, separa válidos × rejeitados e grava |
# MAGIC
# MAGIC **Padrão de cada tabela**
# MAGIC ```
# MAGIC Bronze ─► padroniza texto ─► 1 linha por chave (mais recente) ─► aplica regras
# MAGIC                                                                    ├─ válidos   ─► Silver (MERGE)
# MAGIC                                                                    └─ inválidos ─► _quarentena (com motivo)
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### Etapa 1 — Importações
# MAGIC
# MAGIC **O que faz**
# MAGIC - Importa PySpark (`F`, `Window`) e `DeltaTable` (usado no MERGE).

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 1: IMPORTAÇÕES


from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable



# COMMAND ----------

# MAGIC %md ### Etapa 2 — Credenciais de acesso ao Data Lake
# MAGIC

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 2: CREDENCIAIS DE ACESSO AO DATA LAKE



# Credenciais Azure

client_id =  os.getenv("ADLS_CLIENT_ID")
tenant_id = "os.getenv("ADLS_TENANT_ID")
client_secret = os.getenv("ADLS_CLIENT_SECRET")

storage_account_name = "internshipdatalake"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint":
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

host = f"{storage_account_name}.dfs.core.windows.net"
try:
    for k, v in config.items():
        spark.conf.set(f"{k}.{host}", v)
    print("Credenciais registradas na sessão (MERGE habilitado).")
except Exception as e:
    print(f"Aviso: sessão não aceitou as credenciais ({type(e).__name__}). "
          "MERGE vai cair no fallback de overwrite. Solução definitiva: External Location no Unity Catalog.")

# COMMAND ----------

# MAGIC %md ### Etapa 3 — Caminhos e parâmetros
# MAGIC
# MAGIC **O que faz**
# MAGIC - Caminhos da Bronze (entrada), da Silver e da quarentena (saídas).
# MAGIC - `RECRIAR_TABELAS`: força overwrite (use `True` só na primeira execução da v2).

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 3: CAMINHOS E PARÂMETROS
######################################################################

base_squad3 = f"abfss://squad3@{host}/"

bronze_lojas = base_squad3 + "bronze/physical_lojas"
bronze_produtos = base_squad3 + "bronze/physical_produtos_pereciveis"

silver_lojas = base_squad3 + "silver/physical_lojas"
silver_produtos = base_squad3 + "silver/physical_produtos_pereciveis"

quarentena_lojas = base_squad3 + "silver/_quarentena/physical_lojas"
quarentena_produtos = base_squad3 + "silver/_quarentena/physical_produtos_pereciveis"

# True apenas na 1ª execução(schema novo: _hash_registro)
RECRIAR_TABELAS = False



# COMMAND ----------

# MAGIC %md ### Etapa 4 — Funções de leitura e escrita
# MAGIC
# MAGIC **O que faz**
# MAGIC - `ler_delta` / `existe_delta`: lê e testa se a tabela existe.
# MAGIC - `salvar_silver`: se a tabela existe, faz **MERGE** atualizando só linhas que mudaram (pelo `_hash_registro`); se não existe ou o MERGE falhar, grava com overwrite particionado por `ano`/`mes`.
# MAGIC - `salvar_quarentena`: **acrescenta** os rejeitados (mantém histórico de cada execução).

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 4: FUNÇÕES DE LEITURA E ESCRITA


# [IO]
def ler_delta(caminho):
    return spark.read.format("delta").options(**config).load(caminho)


def existe_delta(caminho):
    try:
        ler_delta(caminho).columns
        return True
    except Exception:
        return False


def _overwrite(df, caminho, particoes):
    (
        df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .options(**config)
            .partitionBy(*particoes)
            .save(caminho)
    )


def salvar_silver(df, caminho, chave, particoes=("ano", "mes")):
    if existe_delta(caminho) and not RECRIAR_TABELAS:
        try:
            alvo = DeltaTable.forPath(spark, caminho)
            (
                alvo.alias("t")
                    .merge(df.alias("s"), f"t.{chave} = s.{chave}")
                    # só reescreve o que mudou -> silver_processed_at/ano/mes estáveis
                    .whenMatchedUpdateAll(condition="t._hash_registro <> s._hash_registro")
                    .whenNotMatchedInsertAll()
                    .execute()
            )
            print(f"MERGE realizado: {caminho}")
            return
        except Exception as e:
            print(f"MERGE indisponível ({type(e).__name__}) -> overwrite.")
    _overwrite(df, caminho, particoes)
    print(f"Tabela Silver gravada (overwrite): {caminho}")


def salvar_quarentena(df, caminho):
    (
        df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .options(**config)
            .save(caminho)
    )
    print(f"Quarentena gravada: {caminho}")


# COMMAND ----------

# MAGIC %md ### Etapa 5 — Funções de qualidade
# MAGIC
# MAGIC **O que faz**
# MAGIC - `normalizar_texto`: tira acento/espaço e põe em maiúsculo.
# MAGIC - `manter_mais_recente`: 1 linha por chave, a de `bronze_ingested_at` mais recente.
# MAGIC - `aplicar_regras`: recebe as regras como `{flag: condição de inválido}` e devolve **(válidos, rejeitados)** com a coluna `_motivo_rejeicao`.
# MAGIC - `auditoria`: adiciona `_hash_registro`, `silver_processed_at`, `ano` e `mes`.
# MAGIC - `checar_pk`: confere chave nula/duplicada e **para o notebook** se encontrar.

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, when, coalesce, sha2, concat_ws, to_json, struct, current_timestamp
)

controle_execucoes = base_squad3 + "silver/_controle/execucoes"
controle_novidades = base_squad3 + "silver/_controle/novidades"
controle_staging = base_squad3 + "silver/_controle/staging/"

EXECUCAO_ID = str(uuid.uuid4())   # identifica esta execução nos logs


def _gravar(df, caminho, modo, particoes=None):
    w = (df.write.format("delta").mode(modo)
           .option("mergeSchema", "true").option("overwriteSchema", "true")
           .options(**config))
    if particoes:
        w = w.partitionBy(*particoes)
    w.save(caminho)


def _hash(df, colunas):
    # "impressão digital" do registro; coluna que não existe entra como nula
    return sha2(concat_ws("||", *[
        coalesce((col(c) if c in df.columns else lit(None)).cast("string"), lit("<nulo>"))
        for c in colunas
    ]), 256)


def upsert_silver(df, caminho_silver, chave):
    """MERGE; se o cluster não deixar, faz o mesmo upsert via overwrite (sem perder dados)."""
    if not existe_delta(caminho_silver):
        _gravar(df, caminho_silver, "overwrite", ["ano", "mes"])
        return "criacao"
    try:
        (
            DeltaTable.forPath(spark, caminho_silver).alias("t")
                .merge(df.alias("s"), f"t.{chave} = s.{chave}")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
        )
        return "merge"
    except Exception as e:
        print(f"MERGE indisponível ({type(e).__name__}) -> upsert via overwrite.")
        atual = ler_delta(caminho_silver)
        resultado = (
            atual.join(df.select(chave), chave, "left_anti")      # mantém quem não mudou
                 .unionByName(df, allowMissingColumns=True)       # + novos e alterados
        )
        _gravar(resultado, caminho_silver, "overwrite", ["ano", "mes"])
        return "upsert_overwrite"


def salvar_silver_monitorado(df, caminho_silver, chave, tabela, colunas):
    """Compara com a Silver, grava só novos/alterados e registra o que mudou."""
    inicio = datetime.now()
    modo, contagem, status, erro = "nenhum", {}, "erro", None
    try:
        # 1. Classifica cada registro: novo / alterado / sem_mudanca
        df = df.withColumn("_hash", _hash(df, colunas))
        if existe_delta(caminho_silver):
            atual = ler_delta(caminho_silver)
            atual = atual.select(col(chave), _hash(atual, colunas).alias("_hash_atual"))
            df = (
                df.join(atual, chave, "left")
                  .withColumn("tipo_mudanca",
                      when(col("_hash_atual").isNull(), "novo")
                      .when(col("_hash") != col("_hash_atual"), "alterado")
                      .otherwise("sem_mudanca"))
                  .drop("_hash_atual")
            )
        else:
            df = df.withColumn("tipo_mudanca", lit("novo"))

        contagem = {r["tipo_mudanca"]: r["count"]
                    for r in df.groupBy("tipo_mudanca").count().collect()}

        # 2. Guarda só as mudanças numa área temporária (staging)
        staging = controle_staging + tabela
        _gravar(df.filter(col("tipo_mudanca") != "sem_mudanca").drop("_hash"), staging, "overwrite")
        mudancas = ler_delta(staging)

        # 3. Grava na Silver e registra QUAIS registros mudaram
        if mudancas.count() > 0:
            modo = upsert_silver(mudancas.drop("tipo_mudanca"), caminho_silver, chave)
            _gravar(
                mudancas.select(
                    lit(tabela).alias("tabela"),
                    col(chave).cast("string").alias("chave"),
                    "tipo_mudanca",
                    to_json(struct(*colunas)).alias("dados"),
                    lit(EXECUCAO_ID).alias("execucao_id"),
                    current_timestamp().alias("detectado_em"),
                ),
                controle_novidades, "append",
            )
        status = "sucesso"
    except Exception as e:
        erro = f"{type(e).__name__}: {str(e)[:900]}"
        raise
    finally:
        # 4. Log da execução (sempre, com sucesso ou erro)
        _gravar(
            spark.createDataFrame(
                [(EXECUCAO_ID, tabela, inicio, datetime.now(), status, modo,
                  contagem.get("novo", 0), contagem.get("alterado", 0),
                  contagem.get("sem_mudanca", 0), erro)],
                "execucao_id string, tabela string, inicio timestamp, fim timestamp, status string, "
                "modo_gravacao string, novos long, alterados long, sem_mudanca long, erro string",
            ),
            controle_execucoes, "append",
        )

    print(f"[{tabela}] novos={contagem.get('novo', 0)} | alterados={contagem.get('alterado', 0)} | "
          f"sem_mudanca={contagem.get('sem_mudanca', 0)} | gravação={modo}")

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 5: FUNÇÕES DE QUALIDADE


_ACENTOS = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_SEM_ACENTOS = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def normalizar_texto(c):
    return F.upper(F.regexp_replace(F.trim(F.translate(c, _ACENTOS, _SEM_ACENTOS)), r"\s+", " "))


def manter_mais_recente(df, chave, ordem="bronze_ingested_at"):
    """Uma linha por chave (a mais recente). Chaves nulas são mantidas para irem à quarentena."""
    w = Window.partitionBy(chave).orderBy(F.col(ordem).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter((F.col("_rn") == 1) | F.col(chave).isNull())
          .drop("_rn")
    )


def aplicar_regras(df, regras):
    """regras = {nome_flag: condição_que_indica_INVALIDO}. Nulo na condição conta como inválido."""
    for nome, cond in regras.items():
        df = df.withColumn(nome, F.coalesce(cond, F.lit(True)))
    motivo = F.concat_ws(
        ", ", *[F.when(F.col(n), F.lit(n.replace("_flag_", ""))) for n in regras]
    )
    df = df.withColumn("_motivo_rejeicao", motivo)
    validos = df.filter(F.col("_motivo_rejeicao") == "").drop("_motivo_rejeicao", *regras.keys())
    rejeitados = df.filter(F.col("_motivo_rejeicao") != "")
    return validos, rejeitados


def auditoria(df, colunas_negocio):
    return (
        df.withColumn(
            "_hash_registro",
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in colunas_negocio]), 256),
        )
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("ano", F.year("silver_processed_at"))
        .withColumn("mes", F.month("silver_processed_at"))
    )


def checar_pk(df, chave, nome):
    nulos = df.filter(F.col(chave).isNull()).count()
    dups = df.groupBy(chave).count().filter("count > 1").count()
    print(f"{nome}: {df.count()} linhas | {chave} nulo={nulos} | duplicados={dups}")
    if nulos or dups:
        raise ValueError(f"PK inválida em {nome}")



# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 2 — `physical_lojas`

# COMMAND ----------

# MAGIC %md ### Etapa 6 — Tratamento e regras de lojas
# MAGIC
# MAGIC **O que faz**
# MAGIC - Padroniza: CNPJ só com dígitos, UF em maiúsculo, nome e cidade sem espaços extras.
# MAGIC - Mantém a versão mais recente de cada `id_loja`.
# MAGIC - Conta quantas lojas usam o mesmo CNPJ.
# MAGIC - Aplica as regras e separa válidos × rejeitados; nos rejeitados com CNPJ de 13 dígitos, explica que provavelmente perdeu o zero à esquerda.
# MAGIC
# MAGIC **Regra atendida:** Lojas T1 (id_loja), T2 (CNPJ 14 dígitos e único) e T3 (UF válida)
# MAGIC
# MAGIC **Gera:** `df_lojas_silver` e `lojas_rejeitadas` (ainda em memória)

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 6: TRATAMENTO E REGRAS DE LOJAS


UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

df_lojas_bronze = ler_delta(bronze_lojas)
print("Bronze physical_lojas:", df_lojas_bronze.count())

lojas_prep = manter_mais_recente(
    df_lojas_bronze
        # cnpj chega como LONG na Bronze -> zeros à esquerda já foram perdidos.
        # Ideal: ingerir cnpj como STRING na Bronze.
        .withColumn("cnpj", F.regexp_replace(F.col("cnpj").cast("string"), "[^0-9]", ""))
        .withColumn("estado_loja", F.upper(F.trim("estado_loja")))
        .withColumn("nome_loja", F.trim("nome_loja"))
        .withColumn("cidade_loja", F.trim("cidade_loja")),
    "id_loja",
).withColumn(
    "_qtd_lojas_mesmo_cnpj",
    F.count("id_loja").over(Window.partitionBy("cnpj")),
)

lojas_validas, lojas_rejeitadas = aplicar_regras(lojas_prep, {
    # T1
    "_flag_id_loja_nulo": F.col("id_loja").isNull(),
    # T2
    "_flag_cnpj_invalido": F.length("cnpj") != 14,
    "_flag_cnpj_duplicado": F.col("_qtd_lojas_mesmo_cnpj") > 1,
    # T3
    "_flag_uf_invalida": ~F.col("estado_loja").isin(UFS_VALIDAS),
})

lojas_rejeitadas = lojas_rejeitadas.withColumn(
    "_observacao",
    F.when(F.length("cnpj") == 13, "cnpj com 13 dígitos: provável zero à esquerda perdido na Bronze"),
)

colunas_lojas = ["id_loja", "nome_loja", "cnpj", "cidade_loja", "estado_loja", "peso_vendas"]
df_lojas_silver = auditoria(lojas_validas.drop("_qtd_lojas_mesmo_cnpj"), colunas_lojas)

print("Silver physical_lojas:", df_lojas_silver.count())
print("Quarentena physical_lojas:", lojas_rejeitadas.count())
display(lojas_rejeitadas.select("id_loja", "nome_loja", "cnpj", "estado_loja", "_motivo_rejeicao", "_observacao"))


# COMMAND ----------

# MAGIC %md ### Etapa 7 — Checagem e gravação de lojas
# MAGIC
# MAGIC **O que faz**
# MAGIC - Confere que `id_loja` e `cnpj` não têm nulos nem duplicatas.
# MAGIC - Grava a Silver (MERGE por `id_loja`) e a quarentena.
# MAGIC
# MAGIC **Gera:** `silver/physical_lojas` e `silver/_quarentena/physical_lojas`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 7: CHECAGEM E GRAVAÇÃO DE LOJAS


checar_pk(df_lojas_silver, "id_loja", "silver physical_lojas")
checar_pk(df_lojas_silver, "cnpj", "silver physical_lojas (cnpj)")

salvar_silver_monitorado(df_lojas_silver, silver_lojas, "id_loja", "physical_lojas", colunas_lojas)
salvar_quarentena(lojas_rejeitadas.withColumn("silver_processed_at", F.current_timestamp()), quarentena_lojas)



# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte 3 — `physical_produtos_pereciveis`

# COMMAND ----------

# MAGIC %md ### Etapa 8 — Tratamento e regras de produtos
# MAGIC
# MAGIC **O que faz**
# MAGIC - Padroniza: SKU sem espaços, unidade em `kg`/`L`/`un`, categoria no nome oficial (aceita variação de acento/caixa), preço em DECIMAL(10,2).
# MAGIC - `nome_marca`: usa a coluna se existir na Bronze; senão extrai do SKU (`categoria-MARCA-tamanho-produto`).
# MAGIC - Mantém a versão mais recente de cada SKU.
# MAGIC - Aplica as regras e separa válidos × rejeitados.
# MAGIC
# MAGIC **Regra atendida:** Perecíveis T1 (SKU), T2 (unidade), T3 (0 < preço ≤ 299,99), T4 (categoria) e T5 (nome_marca)
# MAGIC
# MAGIC **Gera:** `df_produtos_silver` e `produtos_rejeitados` (ainda em memória)
# MAGIC

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 8: TRATAMENTO E REGRAS DE PRODUTOS


CATEGORIAS_PERECIVEIS = [
    "Hortifruti", "Laticínios & Ovos", "Frios & Embutidos",
    "Açougue & Peixaria", "Congelados", "Padaria Fresca",
]
# mapa NOME_NORMALIZADO -> nome canônico (aceita variação de acento/caixa/espaço)
mapa_categorias = F.create_map(*[
    x for c in CATEGORIAS_PERECIVEIS
    for x in (F.lit(c.upper().translate(str.maketrans(_ACENTOS, _SEM_ACENTOS)).upper()), F.lit(c))
])

df_produtos_bronze = ler_delta(bronze_produtos)
print("Bronze physical_produtos_pereciveis:", df_produtos_bronze.count())

# T5: a Bronze atual NÃO tem nome_marca. Enquanto não vier da fonte,
# derivamos do 2º token do SKU (categoria-marca-tamanho-produto).
if "nome_marca" in df_produtos_bronze.columns:
    col_marca, origem_marca = F.trim("nome_marca"), F.lit("fonte")
else:
    col_marca, origem_marca = F.get(F.split(F.trim("sku"), "-"), 1), F.lit("derivado_sku")
    print("Aviso: nome_marca ausente na Bronze -> derivado do SKU.")

produtos_prep = manter_mais_recente(
    df_produtos_bronze
        .withColumn("sku", F.trim("sku"))
        .withColumn(
            "unidade_medida",
            F.when(F.lower(F.trim("unidade_medida")) == "l", "L")
             .otherwise(F.lower(F.trim("unidade_medida"))),
        )
        .withColumn(
            "categoria_pai",
            F.coalesce(mapa_categorias[normalizar_texto(F.col("categoria_pai"))], F.trim("categoria_pai")),
        )
        .withColumn("subcategoria", F.trim("subcategoria"))
        .withColumn("preco_lista", F.col("preco_lista").cast("decimal(10,2)"))
        .withColumn("nome_marca", col_marca)
        .withColumn("nome_marca_origem", origem_marca),
    "sku",
)

produtos_validos, produtos_rejeitados = aplicar_regras(produtos_prep, {
    "_flag_sku_nulo": F.col("sku").isNull() | (F.col("sku") == ""),                       # T1
    "_flag_unidade_invalida": ~F.col("unidade_medida").isin("kg", "L", "un"),              # T2
    "_flag_preco_invalido": ~((F.col("preco_lista") > 0) & (F.col("preco_lista") <= 299.99)),  # T3
    "_flag_categoria_invalida": ~F.col("categoria_pai").isin(CATEGORIAS_PERECIVEIS),       # T4
    "_flag_marca_vazia": F.col("nome_marca").isNull() | (F.col("nome_marca") == ""),       # T5
})

colunas_produtos = ["sku", "categoria_pai", "subcategoria", "preco_lista", "unidade_medida", "nome_marca"]
df_produtos_silver = auditoria(produtos_validos, colunas_produtos)

print("Silver physical_produtos_pereciveis:", df_produtos_silver.count())
print("Quarentena physical_produtos_pereciveis:", produtos_rejeitados.count())
display(produtos_rejeitados.select("sku", "categoria_pai", "unidade_medida", "preco_lista", "_motivo_rejeicao"))



# COMMAND ----------

# MAGIC %md ### Etapa 9 — Checagem e gravação de produtos
# MAGIC
# MAGIC **O que faz**
# MAGIC - Confere que `sku` não tem nulos nem duplicatas.
# MAGIC - Grava a Silver (MERGE por `sku`) e a quarentena.
# MAGIC
# MAGIC **Gera:** `silver/physical_produtos_pereciveis` e `silver/_quarentena/physical_produtos_pereciveis`

# COMMAND ----------

######################################################################
# ▶ INÍCIO — ETAPA 9: CHECAGEM E GRAVAÇÃO DE PRODUTOS


checar_pk(df_produtos_silver, "sku", "silver physical_produtos_pereciveis")

salvar_silver_monitorado(df_produtos_silver, silver_produtos, "sku", "physical_produtos_pereciveis", colunas_produtos)
salvar_quarentena(produtos_rejeitados.withColumn("silver_processed_at", F.current_timestamp()), quarentena_produtos)


# COMMAND ----------

# Últimas execuções: quantos novos / alterados / sem mudança em cada rodada
if existe_delta(controle_execucoes):
    display(
        ler_delta(controle_execucoes)
            .select("inicio", "tabela", "status", "modo_gravacao",
                    "novos", "alterados", "sem_mudanca", "erro")
            .orderBy(col("inicio").desc())
            .limit(20)
    )
else:
    print("Nenhuma execução registrada ainda. Rode as Etapas 7 e 9 primeiro.")

# Quais registros entraram ou mudaram na última vez em que houve novidade
if existe_delta(controle_novidades):
    nov = ler_delta(controle_novidades)
    display(
        nov.withColumn("_ultima", F.max("detectado_em").over(Window.partitionBy("tabela")))
           .filter(col("detectado_em") == col("_ultima"))
           .select("tabela", "tipo_mudanca", "chave", "dados", "detectado_em")
           .orderBy("tabela", "tipo_mudanca", "chave")
    )
else:
    print("Nenhum dado novo ou alterado até agora: a Silver já está igual à Bronze.")