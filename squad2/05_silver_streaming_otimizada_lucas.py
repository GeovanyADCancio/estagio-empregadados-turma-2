# Databricks notebook source
# 05 - Camada Silver — Ingestão Incremental com MERGE em Delta Lake
# Squad 2 / Dupla 1 — Autor: Lucas Sousa Santos Oliveira
# Tabelas de escopo: ecommerce_produtos (chave: sku) e ecommerce_categorias (chave: id_categoria)
# Fonte de dados: camada Bronze em Delta Lake, já ingerida no namespace squad2/grupo1/bronze/*
# Fonte de regras: planilha oficial da gestão técnica (regras_estagio_engdados_squad2_.xlsx,
# aba "Squad 2", linhas ecommerce_produtos e ecommerce_categorias) — única fonte normativa
# de validação técnica e de negócio usada neste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivo e arquitetura
# MAGIC
# MAGIC 1. **Leitura incremental da Bronze via Structured Streaming** com
# MAGIC    `trigger(availableNow=True)`: processa só o que ainda não foi lido, usando
# MAGIC    checkpoint de progresso — não escaneia a tabela Silver inteira para descobrir
# MAGIC    o que é novo.
# MAGIC 2. **Persistência via `MERGE INTO` (Delta Lake)**, não `append`: cada micro-lote é
# MAGIC    casado contra a Silver pela chave de negócio (`sku` / `id_categoria`).
# MAGIC    Registro já existente é atualizado (`UPDATE`); registro novo é inserido
# MAGIC    (`INSERT`). Isso garante que reprocessamento do mesmo dado (replay de
# MAGIC    checkpoint, arquivo Bronze reenviado) não duplica linhas na Silver — o efeito é
# MAGIC    idempotente, diferente de um `append` puro.
# MAGIC 3. **Validação técnica e de negócio** aplicada antes do `MERGE`: registros
# MAGIC    reprovados vão para uma tabela de quarentena separada (também via `MERGE`,
# MAGIC    pela mesma razão de idempotência) e nunca chegam à Silver.
# MAGIC 4. **Sem particionamento físico na Silver**: produtos e categorias são tabelas de
# MAGIC    dimensão pequenas; em vez de particionar, uso `OPTIMIZE ... ZORDER BY` para
# MAGIC    acelerar consultas por chave sem gerar excesso de arquivos pequenos.
# MAGIC 5. **`autoOptimize` ligado** nas tabelas Delta para compactação automática de
# MAGIC    arquivos pequenos gerados por micro-lotes frequentes.

# COMMAND ----------

# MAGIC %md ## 1. Credenciais e configuração de conexão

# COMMAND ----------

import os                                                    # acesso a variáveis de ambiente — nenhuma credencial hardcoded no notebook
from dotenv import load_dotenv, find_dotenv                  # find_dotenv localiza o .env subindo diretórios, sem path fixo

dotenv_path = find_dotenv()                                   # busca automática do arquivo .env no workspace
load_dotenv(dotenv_path, override=True)                       # override=True garante o valor atual do arquivo, não env residual da sessão

storage_account = os.getenv("ADLS_STORAGE_ACCOUNT_NAME", "internshipdatalake")  # nome da conta ADLS Gen2 do datalake do squad
client_id = os.getenv("ADLS_CLIENT_ID")                       # Service Principal — client id
tenant_id = os.getenv("ADLS_TENANT_ID")                       # Service Principal — tenant do Azure AD
client_secret = os.getenv("ADLS_CLIENT_SECRET")               # Service Principal — secret (nunca logado em print)

print("Camada Silver — variáveis de ambiente carregadas:")    # log mínimo de diagnóstico, sem expor valores sensíveis
print(f"  Storage Account: {storage_account}")                # confirma qual conta está sendo usada (detecta .env apontando errado)
print(f"  Credenciais presentes: {all([client_id, tenant_id, client_secret])}")  # bool único — presença, não validade

# COMMAND ----------

# MAGIC %md ## 2. OAuth granular, caminhos Delta e propriedades de otimização de tabela

# COMMAND ----------

# Configuração OAuth do Service Principal, aplicada por conta de storage — compatível
# com Databricks Serverless (não depende de mount global de cluster).
adls_options = {
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": client_id,
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

for chave, valor in adls_options.items():                     # registra cada config OAuth na SparkSession ativa
    spark.conf.set(chave, valor)                               # necessário para readStream e para DeltaTable.forPath enxergarem o storage

base_squad = f"abfss://squad2@{storage_account}.dfs.core.windows.net/grupo1"  # namespace seguro do squad2 no Data Lake

caminhos = {
    "bronze_produtos": f"{base_squad}/bronze/ecommerce_produtos",         # origem: camada Bronze já ingerida no Data Lake
    "bronze_categorias": f"{base_squad}/bronze/ecommerce_categorias",     # origem: camada Bronze já ingerida no Data Lake
    "silver_produtos": f"{base_squad}/silver/ecommerce_produtos",         # destino: Silver de produtos (chave de merge: sku)
    "silver_categorias": f"{base_squad}/silver/ecommerce_categorias",     # destino: Silver de categorias (chave de merge: id_categoria)
    "quarantine_produtos": f"{base_squad}/quarantine/ecommerce_produtos", # destino: registros reprovados nas regras técnicas
    "quarantine_categorias": f"{base_squad}/quarantine/ecommerce_categorias",
    # Checkpoints do Structured Streaming: guardam o progresso de leitura da Bronze
    # (quais arquivos/versões já foram lidos), eliminando a necessidade de escanear
    # a Silver para descobrir o que falta processar.
    "checkpoint_produtos": f"{base_squad}/_checkpoints/silver_ecommerce_produtos",
    "checkpoint_categorias": f"{base_squad}/_checkpoints/silver_ecommerce_categorias",
}

# Propriedades Delta de auto-otimização de escrita:
# - optimizeWrite: compacta arquivos pequenos já no momento do commit da escrita
# - autoCompact: roda compactação leve automaticamente após cada write, sem job manual
DELTA_AUTO_OPTIMIZE = {
    "delta.autoOptimize.optimizeWrite": "true",   # crítico em MERGE de micro-lotes pequenos e frequentes
    "delta.autoOptimize.autoCompact": "true",     # evita acúmulo de arquivos pequenos sem operação manual
}

print("Caminhos da camada Silver configurados:")               # confirma os paths antes de qualquer leitura/escrita
for k, v in caminhos.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md ## 3. Schema explícito da Bronze e utilitário de existência de tabela

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType
)  # tipos explícitos: Structured Streaming não deve depender de inferência de schema
   # (inferir a cada start é mais lento e menos estável em produção)

# Schema da Bronze de produtos: colunas de negócio + colunas de auditoria/partição gravadas na ingestão Bronze.
schema_bronze_produtos = StructType([
    StructField("sku", StringType(), True),                   # chave de negócio — usada como chave do MERGE
    StructField("nome_produto", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("id_categoria", StringType(), True),          # FK lógica para ecommerce_categorias
    StructField("unidade_medida", StringType(), True),
    StructField("nome_marca", StringType(), True),
    StructField("preco_lista", DoubleType(), True),           # recebido como double; re-casto por segurança adiante
    StructField("is_ativo", BooleanType(), True),
    StructField("bronze_source_file", StringType(), True),    # metadado de auditoria da ingestão Bronze
    StructField("bronze_ingested_at", TimestampType(), True), # timestamp de ingestão na Bronze — carregado para auditoria na Silver
    StructField("ano", IntegerType(), True),                  # coluna de partição física da Bronze (não usada na Silver)
    StructField("mes", IntegerType(), True),
    StructField("dia", IntegerType(), True),
    StructField("hora", IntegerType(), True),
])

# Schema da Bronze de categorias — mesmo raciocínio.
schema_bronze_categorias = StructType([
    StructField("id_categoria", StringType(), True),          # chave de negócio — usada como chave do MERGE
    StructField("nome_categoria", StringType(), True),
    StructField("id_categoria_pai", StringType(), True),
    StructField("nome_categoria_pai", StringType(), True),
    StructField("tipo_categoria", StringType(), True),
    StructField("bronze_source_file", StringType(), True),
    StructField("bronze_ingested_at", TimestampType(), True),
    StructField("ano", IntegerType(), True),
    StructField("mes", IntegerType(), True),
    StructField("dia", IntegerType(), True),
    StructField("hora", IntegerType(), True),
])


def tabela_delta_existe(caminho: str) -> bool:
    """Verifica existência física da tabela pelo log de transação Delta, sem
    disparar leitura de dado — muito mais barato que um read + count()."""
    from delta.tables import DeltaTable                        # import local: só carrega a lib quando a função é de fato chamada
    return DeltaTable.isDeltaTable(spark, caminho)              # checagem de metadado (log _delta_log), não de conteúdo

# COMMAND ----------

# MAGIC %md ## 4. Função genérica de upsert via `MERGE INTO`

# COMMAND ----------

from delta.tables import DeltaTable                            # API de tabela Delta — necessária para construir o MERGE


def upsert_delta(df_origem, caminho_destino: str, coluna_chave: str) -> None:
    """
    Grava df_origem no destino Delta usando MERGE (upsert) pela coluna_chave.
    - Se a tabela destino ainda não existe fisicamente, faz a escrita inicial via
      `write` (não há o que casar num MERGE contra uma tabela inexistente).
    - Se já existe, casa por coluna_chave: registro correspondente é atualizado
      (UPDATE), registro sem correspondência é inserido (INSERT). Isso torna a
      escrita idempotente — reprocessar o mesmo micro-lote não duplica linhas.
    """
    if not tabela_delta_existe(caminho_destino):                # primeira carga: ainda não há tabela para fazer MERGE contra
        (df_origem.write
            .format("delta")                                    # formato Delta Lake explícito — exigência da task
            .option("mergeSchema", "false")                      # schema fixo: mudança inesperada deve falhar visivelmente, não ser absorvida
            .save(caminho_destino))                               # cria a tabela Delta pela primeira vez
        print(f"    -> tabela criada em {caminho_destino} (carga inicial via write).")
        return                                                    # encerra aqui: não há MERGE a fazer na primeira carga

    tabela_destino = DeltaTable.forPath(spark, caminho_destino)   # referência à tabela Delta já existente no Data Lake

    (tabela_destino.alias("destino")                              # alias "destino" identifica a tabela Silver dentro da condição de match
        .merge(
            df_origem.alias("origem"),                            # alias "origem" identifica o micro-lote recém-validado da Bronze
            f"destino.{coluna_chave} = origem.{coluna_chave}",     # condição de casamento — chave de negócio, não chave técnica sintética
        )
        .whenMatchedUpdateAll()                                   # registro já existente: atualiza todas as colunas com o valor mais recente
        .whenNotMatchedInsertAll()                                # registro novo: insere a linha inteira
        .execute())                                                # dispara o plano físico do MERGE (leitura + join + escrita atômica)

    print(f"    -> MERGE aplicado em {caminho_destino} (upsert por '{coluna_chave}').")

# COMMAND ----------

# MAGIC %md ## 5. Função de processamento — `ecommerce_produtos` (chamada por micro-lote via `foreachBatch`)

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp, col, trim, length, when, lit, concat_ws,
    countDistinct, broadcast,
)  # broadcast: hint explícito para evitar shuffle em join contra tabela de dimensão pequena
from pyspark.sql import DataFrame


def processar_lote_produtos(df_microlote: DataFrame, id_lote: int) -> None:
    """
    Executada pelo Structured Streaming a cada micro-lote disponível na Bronze.
    O corpo roda como um DataFrame estático (foreachBatch expõe batch, não streaming
    nativo), permitindo usar MERGE normalmente dentro da função.
    """
    total_bruto = df_microlote.count()                          # única leitura física deste micro-lote (base para o cache logo abaixo)
    if total_bruto == 0:                                        # availableNow pode disparar um lote vazio ao fim do backlog
        print(f"[lote {id_lote}] vazio — nada a processar em ecommerce_produtos.")
        return                                                  # evita abrir writer/MERGE à toa

    print(f"[lote {id_lote}] ecommerce_produtos — {total_bruto} registros recebidos da Bronze")

    # Cache: df_microlote é usado por várias ações (count, join, MERGE) nesta função.
    # Sem cache, cada ação relançaria a leitura+filtro desde a Bronze no Data Lake.
    df_microlote = df_microlote.cache()

    # 1. Auditoria + limpeza de strings (trim) + tipagem estrita (cast).
    #    Cast que falha vira NULL em Spark — por isso a validação abaixo trata NULL
    #    como falha explícita, nunca como "passou no range check".
    df_limpo = (
        df_microlote
        .withColumn("silver_processed_at", current_timestamp())      # timestamp de quando esta execução processou o registro
        .withColumn("sku", trim(col("sku")))                          # remove espaços de borda
        .withColumn("nome_produto", trim(col("nome_produto")))
        .withColumn("descricao", trim(col("descricao")))
        .withColumn("id_categoria", trim(col("id_categoria")))
        .withColumn("unidade_medida", trim(col("unidade_medida")))
        .withColumn("nome_marca", trim(col("nome_marca")))
        .withColumn("preco_lista", col("preco_lista").cast(DoubleType()))   # cast defensivo
        .withColumn("is_ativo", col("is_ativo").cast(BooleanType()))
    )

    # 2. Regras TÉCNICAS (planilha oficial, aba Squad 2, tabela ecommerce_produtos):
    #    Técnica 1: 5 < len(sku) < 60      Técnica 2: 0 < preco_lista < 5000
    #    Técnica 3: is_ativo boolean, sem nulo.
    #    Cada condição trata NULL como falha explícita: em lógica de três valores do
    #    Spark, `NULL > 0` retorna NULL (não False), então checar `isNotNull()`
    #    primeiro evita que um valor nulo escape da quarentena por engano.
    cond_sku_valido = (
        col("sku").isNotNull()
        & (length(col("sku")) > 5)
        & (length(col("sku")) < 60)
    )
    cond_preco_valido = (
        col("preco_lista").isNotNull()
        & (col("preco_lista") > 0)
        & (col("preco_lista") < 5000)
    )
    cond_ativo_valido = col("is_ativo").isNotNull()

    df_marcado = df_limpo.withColumn(
        "quarantine_reason",
        concat_ws("; ",
            when(~cond_sku_valido, lit("FALHA_TECNICA_1: SKU nulo ou fora da faixa de 5 a 60 caracteres")),
            when(~cond_preco_valido, lit("FALHA_TECNICA_2: preco_lista nulo ou fora da faixa de 0 a 5000")),
            when(~cond_ativo_valido, lit("FALHA_TECNICA_3: is_ativo nulo ou tipo inválido")),
        ),
    ).withColumn("quarantined_at", current_timestamp())                # timestamp com sentido apenas para quem cai na quarentena

    df_validos = df_marcado.filter(col("quarantine_reason") == "").drop("quarantine_reason", "quarantined_at")
    df_quarentena = df_marcado.filter(col("quarantine_reason") != "")

    total_validos = df_validos.count()                                  # reaproveita o cache herdado de df_microlote
    total_quarentena = df_quarentena.count()
    print(f"    válidos={total_validos}  quarentena={total_quarentena}")

    # 3. Regra de NEGÓCIO 5: alerta se produto ATIVO tiver preco_lista <= 0 (ou nulo).
    #    Calculada sobre df_limpo (pré-quarentena): é um alerta operacional sobre o
    #    que chegou no lote, independente do destino final do registro.
    qtd_ativo_preco_invalido = df_limpo.filter(
        (col("is_ativo") == True) & (col("preco_lista").isNull() | (col("preco_lista") <= 0))
    ).count()
    if qtd_ativo_preco_invalido > 0:
        print(f"    🚨 [NEGÓCIO 5] {qtd_ativo_preco_invalido} produto(s) ATIVO(s) com preço <= 0 ou nulo neste lote!")

    # 4. Regra de NEGÓCIO 4: SKUs novos no lote (comparado à Silver atual).
    #    Broadcast join: a Silver de produtos é uma tabela de dimensão pequena
    #    (catálogo de e-commerce), então forçar broadcast evita shuffle desnecessário.
    if tabela_delta_existe(caminhos["silver_produtos"]):
        df_skus_existentes = (
            spark.read.format("delta").load(caminhos["silver_produtos"])
            .select("sku")                                            # projeção mínima — só a coluna usada no join, reduz o broadcast
            .distinct()
        )
        skus_novos = (
            df_validos.join(broadcast(df_skus_existentes), on="sku", how="left_anti")
            .select(countDistinct("sku")).first()[0]
        )
    else:
        skus_novos = df_validos.select(countDistinct("sku")).first()[0]   # primeira carga: todo SKU válido é "novo"

    print(f"    📊 [NEGÓCIO 4] SKUs novos neste lote: {skus_novos}")
    if skus_novos > 50:                                                   # limite definido na planilha oficial
        print(f"    ⚠️ [NEGÓCIO 4] lote com mais de 50 SKUs novos — verificar se é carga de teste.")

    # 5. Persistência via MERGE (upsert por sku) — idempotente por construção.
    if total_validos > 0:
        upsert_delta(df_validos, caminhos["silver_produtos"], coluna_chave="sku")

    # 6. Quarentena também via MERGE, pela mesma chave de negócio — evita duplicar
    #    o mesmo registro reprovado se o micro-lote for reprocessado.
    if total_quarentena > 0:
        upsert_delta(df_quarentena, caminhos["quarantine_produtos"], coluna_chave="sku")

    df_microlote.unpersist()                                           # libera a memória do executor ao final do ciclo deste micro-lote

# COMMAND ----------

# MAGIC %md ## 6. Função de processamento — `ecommerce_categorias`

# COMMAND ----------

def processar_lote_categorias(df_microlote: DataFrame, id_lote: int) -> None:
    total_bruto = df_microlote.count()                                  # única leitura física deste micro-lote
    if total_bruto == 0:
        print(f"[lote {id_lote}] vazio — nada a processar em ecommerce_categorias.")
        return

    print(f"[lote {id_lote}] ecommerce_categorias — {total_bruto} registros recebidos da Bronze")
    df_microlote = df_microlote.cache()                                 # reaproveitado por count/join/MERGE abaixo

    # 1. Auditoria + limpeza de strings.
    df_limpo = (
        df_microlote
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("id_categoria", trim(col("id_categoria")))
        .withColumn("nome_categoria", trim(col("nome_categoria")))
        .withColumn("id_categoria_pai", trim(col("id_categoria_pai")))
        .withColumn("nome_categoria_pai", trim(col("nome_categoria_pai")))
        .withColumn("tipo_categoria", trim(col("tipo_categoria")))
    )

    # 2. Regra TÉCNICA 1 (planilha oficial): id_categoria e nome_categoria obrigatórios.
    #    isNotNull() nunca retorna NULL, então o `&` aqui já se comporta corretamente
    #    mesmo com valor nulo (curto-circuita para False, não para NULL).
    cond_id_valido = col("id_categoria").isNotNull() & (length(col("id_categoria")) > 0)
    cond_nome_valido = col("nome_categoria").isNotNull() & (length(col("nome_categoria")) > 0)

    df_marcado = df_limpo.withColumn(
        "quarantine_reason",
        concat_ws("; ",
            when(~cond_id_valido, lit("FALHA_TECNICA_1: id_categoria nulo ou vazio")),
            when(~cond_nome_valido, lit("FALHA_TECNICA_1: nome_categoria nulo ou vazio")),
        ),
    ).withColumn("quarantined_at", current_timestamp())

    df_validos = df_marcado.filter(col("quarantine_reason") == "").drop("quarantine_reason", "quarantined_at")
    df_quarentena = df_marcado.filter(col("quarantine_reason") != "")

    total_validos = df_validos.count()
    total_quarentena = df_quarentena.count()
    print(f"    válidos={total_validos}  quarentena={total_quarentena}")

    # 3. Regra de NEGÓCIO 2: alerta se o número de categorias raiz mudar entre lotes.
    cat_raiz_lote = df_validos.filter(
        col("id_categoria_pai").isNull() | (col("id_categoria_pai") == "")
    ).select(countDistinct("id_categoria")).first()[0]
    print(f"    🌳 [NEGÓCIO 2] categorias raiz neste lote: {cat_raiz_lote}")

    if tabela_delta_existe(caminhos["silver_categorias"]):
        df_silver_cat = (
            spark.read.format("delta").load(caminhos["silver_categorias"])
            .select("id_categoria", "id_categoria_pai")                # projeção mínima — só as colunas necessárias para o KPI
        )
        cat_raiz_existentes = df_silver_cat.filter(
            col("id_categoria_pai").isNull() | (col("id_categoria_pai") == "")
        ).select(countDistinct("id_categoria")).first()[0]
        if cat_raiz_existentes > 0 and cat_raiz_lote != cat_raiz_existentes:
            print(f"    ⚠️ [NEGÓCIO 2] total de categorias raiz mudou (antes: {cat_raiz_existentes}, agora: {cat_raiz_lote}) — impacto na navegação do e-commerce!")

    # 4. Persistência via MERGE (upsert por id_categoria).
    if total_validos > 0:
        upsert_delta(df_validos, caminhos["silver_categorias"], coluna_chave="id_categoria")

    if total_quarentena > 0:
        upsert_delta(df_quarentena, caminhos["quarantine_categorias"], coluna_chave="id_categoria")

    df_microlote.unpersist()                                            # libera cache do executor

# COMMAND ----------

# MAGIC %md ## 7. Execução incremental via Structured Streaming (`trigger(availableNow=True)`)
# MAGIC
# MAGIC `availableNow=True` processa o que existe hoje na Bronze desde o último
# MAGIC checkpoint e encerra sozinho — comportamento de job incremental agendado, com
# MAGIC controle de progresso automático via checkpoint (sem precisar consultar a Silver).

# COMMAND ----------

print("Iniciando processamento incremental de ecommerce_produtos...")   # log de início — facilita localizar o ponto de falha em job agendado

query_produtos = (
    spark.readStream                                                    # leitor de streaming — habilita checkpoint de progresso
    .format("delta")
    .schema(schema_bronze_produtos)                                     # schema explícito: evita inferência a cada start
    .load(caminhos["bronze_produtos"])                                  # fonte: Bronze em Delta Lake
    .writeStream
    .foreachBatch(processar_lote_produtos)                              # delega cada micro-lote para a função de validação + MERGE
    .option("checkpointLocation", caminhos["checkpoint_produtos"])      # progresso persistido — controla o que já foi lido da Bronze
    .trigger(availableNow=True)                                         # processa o backlog disponível agora e encerra
    .start()
)
query_produtos.awaitTermination()                                       # bloqueia até este ciclo de processamento terminar

print("\nIniciando processamento incremental de ecommerce_categorias...")

query_categorias = (
    spark.readStream
    .format("delta")
    .schema(schema_bronze_categorias)
    .load(caminhos["bronze_categorias"])
    .writeStream
    .foreachBatch(processar_lote_categorias)
    .option("checkpointLocation", caminhos["checkpoint_categorias"])
    .trigger(availableNow=True)
    .start()
)
query_categorias.awaitTermination()

print("\nProcessamento incremental da Silver concluído (produtos + categorias).")

# COMMAND ----------

# MAGIC %md ## 8. Registro no Metastore + ativação de auto-otimização das tabelas Delta

# COMMAND ----------

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS squad2")                     # idempotente — não falha se o schema já existir

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS squad2.silver_ecommerce_produtos
        USING DELTA
        LOCATION '{caminhos["silver_produtos"]}'
    """)                                                                # tabela externa — aponta para o Data Lake, não duplica dado no Metastore

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS squad2.silver_ecommerce_categorias
        USING DELTA
        LOCATION '{caminhos["silver_categorias"]}'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS squad2.quarantine_ecommerce_produtos
        USING DELTA
        LOCATION '{caminhos["quarantine_produtos"]}'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS squad2.quarantine_ecommerce_categorias
        USING DELTA
        LOCATION '{caminhos["quarantine_categorias"]}'
    """)

    # Ativa optimizeWrite/autoCompact nas 4 tabelas já criadas — efeito prático:
    # menos arquivos pequenos por micro-lote, leitura futura mais rápida e barata.
    for tabela in [
        "squad2.silver_ecommerce_produtos",
        "squad2.silver_ecommerce_categorias",
        "squad2.quarantine_ecommerce_produtos",
        "squad2.quarantine_ecommerce_categorias",
    ]:
        for propriedade, valor in DELTA_AUTO_OPTIMIZE.items():          # aplica as duas propriedades em cada uma das 4 tabelas
            spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('{propriedade}' = '{valor}')")

    print("Tabelas Silver/Quarentena registradas e auto-otimização ativada.")
except Exception as e:                                                  # Metastore externo pode não estar acessível em todo ambiente de teste
    print(f"Nota sobre Metastore: {e}")
    print("Dados gravados normalmente no Data Lake; registro de tabela externa pode ser refeito depois.")

# COMMAND ----------

# MAGIC %md ## 9. Manutenção Delta (job separado/agendado — não roda a cada micro-lote)
# MAGIC
# MAGIC `OPTIMIZE` reorganiza fisicamente os arquivos Parquet da tabela Delta;
# MAGIC `ZORDER BY` co-localiza os dados pela coluna mais usada em filtro/join
# MAGIC (`sku`, `id_categoria`), acelerando essas consultas sem depender de partição.
# MAGIC Executar isso a cada micro-lote pequeno desperdiçaria compute — o correto é
# MAGIC uma rotina de manutenção periódica (ex.: 1x por dia), separada da ingestão.

# COMMAND ----------

spark.sql("OPTIMIZE squad2.silver_ecommerce_produtos ZORDER BY (sku)")              # compacta e ordena fisicamente por sku
spark.sql("OPTIMIZE squad2.silver_ecommerce_categorias ZORDER BY (id_categoria)")   # idem, pela chave de categoria

print("Manutenção OPTIMIZE/ZORDER concluída na camada Silver.")

# COMMAND ----------

# MAGIC %md ## 10. Auditoria pós-carga

# COMMAND ----------

print("=== Relatório de Auditoria — Camada Silver ===\n")

if tabela_delta_existe(caminhos["silver_produtos"]):
    df_silver_prod = spark.read.format("delta").load(caminhos["silver_produtos"])   # leitura isolada para o relatório final
    total_prod = df_silver_prod.count()
    unicos_prod = df_silver_prod.select(countDistinct("sku")).first()[0]
    print(f"Produtos na Silver: {total_prod} registros ({unicos_prod} SKUs únicos)")
    display(df_silver_prod.select("sku", "nome_produto", "preco_lista", "is_ativo", "silver_processed_at").limit(5))
else:
    print("Silver de produtos ainda sem dados.")

if tabela_delta_existe(caminhos["silver_categorias"]):
    df_silver_cat = spark.read.format("delta").load(caminhos["silver_categorias"])
    total_cat = df_silver_cat.count()
    unicos_cat = df_silver_cat.select(countDistinct("id_categoria")).first()[0]
    print(f"\nCategorias na Silver: {total_cat} registros ({unicos_cat} categorias únicas)")
    display(df_silver_cat.select("id_categoria", "nome_categoria", "tipo_categoria", "silver_processed_at").limit(5))
else:
    print("\nSilver de categorias ainda sem dados.")

if tabela_delta_existe(caminhos["quarantine_produtos"]):
    df_quar_prod = spark.read.format("delta").load(caminhos["quarantine_produtos"])
    print(f"\nProdutos em quarentena: {df_quar_prod.count()} registros")
    display(df_quar_prod.select("sku", "preco_lista", "is_ativo", "quarantine_reason").limit(5))

if tabela_delta_existe(caminhos["quarantine_categorias"]):
    df_quar_cat = spark.read.format("delta").load(caminhos["quarantine_categorias"])
    print(f"\nCategorias em quarentena: {df_quar_cat.count()} registros")
