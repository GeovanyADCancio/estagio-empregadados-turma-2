# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## 🧾 Ficha de identificação
# MAGIC
# MAGIC | Informação | Detalhe |
# MAGIC |---|---|
# MAGIC | **Notebook** | `03_bronze_ingestao_delta_squad2_dupla1` |
# MAGIC | **Camada** | Bronze |
# MAGIC | **Tabelas / saídas principais** | `bronze/ecommerce_produtos`, `bronze/ecommerce_categorias` (Delta) |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | DataFrames extraídos pelo notebook `01` (via `%run`) |
# MAGIC | **Destino** | ADLS Gen2 — container `squad2`, tabelas Delta da camada Bronze |
# MAGIC | **Motivo** | Persistir o dado bruto de forma idempotente e auditável, sem validar regra de negócio |
# MAGIC | **Última atualização** | 08/10/2026 |
# MAGIC | **Observações** | Append-only, sem deduplicação — a versão vigente é decidida na Silver (`04`) |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 29/09/2026 | Lucas Sousa | Criação do notebook Bronze |
# MAGIC | 03/10/2026 | Lucas Sousa, Zaiden Seleme | Remoção da função que movia/renomeava arquivos no RAW — bug crítico reportado por Zaiden |
# MAGIC | 07/10/2026 | Lucas Sousa | Filtro de duplicidade trocado de `isin()` para `left_anti` join; adição do manifesto de ingestão e de checks de Data Quality |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 · Camada Bronze (Delta) — ingestão append-only
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC Ingere o que o `01` extraiu da origem RAW para tabelas Delta, em modo append-only. Não há
# MAGIC validação de regra de negócio nesta camada — isso é responsabilidade da Silver (`04`); aqui a
# MAGIC única responsabilidade é persistir o dado bruto de forma idempotente e auditável.
# MAGIC
# MAGIC ## Garantias operacionais desta camada
# MAGIC
# MAGIC | Garantia | Como é implementada |
# MAGIC |---|---|
# MAGIC | **Append puro, sem deduplicação** | Bronze é log bruto; a Silver decide a versão vigente |
# MAGIC | **Idempotência em duas travas** | (1) o `01` só entrega arquivos ausentes dos manifestos de ingestão; (2) aqui, anti-join por `arquivo_origem` (caminho completo) contra o próprio Bronze. O nome do arquivo não serve de chave, pois se repete em toda janela |
# MAGIC | **Origem RAW somente leitura** | O container RAW pertence ao produtor dos dados. Este pipeline só lê dele; nunca escreve, move ou renomeia. A decisão de "já processado" vem dos manifestos JSON (`controle/ingestao/`) e do Bronze, ambos no container do Lakehouse. Não existe Landing Zone própria |
# MAGIC | **Manifesto de ingestão** | Um JSON por execução e tabela em `controle/ingestao/<tabela>/<run_id>.json`, gravado **depois** do append: arquivo, tamanho e data no ADLS, linhas e status (`INGERIDO`, `VAZIO`, `JA_NO_BRONZE`). Falha no meio nunca marca arquivo como ingerido |
# MAGIC | **Log e Data Quality** | `log_pipeline` (volume, status, tempo, erro) e `log_data_quality` (checks de volume, completude e schema), ambos por `run_id` |
# MAGIC | **Evolução de schema aditiva e deliberada** | `mergeSchema` só liga quando há coluna nova de verdade — drift inesperado quebra visivelmente |
# MAGIC | **Status estruturado pro orquestrador** | Falha vira exceção (Job só marca *Failed* com exceção não tratada); sucesso devolve resumo via `dbutils.notebook.exit` |
# MAGIC
# MAGIC ## Checks de Data Quality desta camada
# MAGIC
# MAGIC | Check | Tipo | Reprova quando |
# MAGIC |---|---|---|
# MAGIC | `janelas_extraidas_sem_falha` | COMPLETUDE | alguma janela da origem RAW não pôde ser lida (fica fora do Bronze e é retentada) |
# MAGIC | `janelas_com_linhas` | VOLUME | alguma janela extraída chegou sem nenhuma linha |
# MAGIC | `schema_sem_coluna_nova` | TECNICA | o lote trouxe coluna que o Bronze ainda não tinha (deriva de schema) |
# MAGIC
# MAGIC ## Dependências
# MAGIC Herda `00_config` (configuração e utilitários) e `df_*`/`arquivos_*`/`meta_landing`/`linhas_por_arquivo`
# MAGIC via `01`, onde `arquivos_*` já contém somente os arquivos ainda não ingeridos.

# COMMAND ----------

# MAGIC %run ./01_conexao_adls_squad2_dupla1_Parte_Lucas

# COMMAND ----------

# MAGIC %md ## Metadados de ingestão

# COMMAND ----------

import json

from pyspark.sql import DataFrame, functions as F

NOTEBOOK = "03_bronze_ingestao_delta_squad2_dupla1"


def adicionar_metadados_ingestao(df: DataFrame) -> DataFrame:
    """Um timestamp por lote. data_ingestao é o corte de leitura incremental da Silver, por isso
    é a única origem dos demais campos temporais.

    bronze_source_file guarda a janela + arquivo (AAAA/MM/DD/HHMMSS/arquivo.parquet): o nome do
    arquivo isolado é constante entre janelas e não carregaria informação."""
    janela_arquivo = F.regexp_extract("arquivo_origem", r"(\d{4}/\d{2}/\d{2}/\d{6}/[^/]+)$", 1)
    return (
        df.withColumn("data_ingestao", F.current_timestamp())
          .withColumn("data_particao", F.to_date("data_ingestao"))
          .withColumn("bronze_ingested_at", F.col("data_ingestao"))
          .withColumn(
              "bronze_source_file",
              F.when(janela_arquivo != "", janela_arquivo)
               .otherwise(F.element_at(F.split("arquivo_origem", "/"), -1)),
          )
          .withColumn("ano", F.year("data_ingestao"))
          .withColumn("mes", F.month("data_ingestao"))
          .withColumn("dia", F.dayofmonth("data_ingestao"))
          .withColumn("hora", F.hour("data_ingestao"))
    )

# COMMAND ----------

# MAGIC %md ## Ingestão por tabela
# MAGIC
# MAGIC Ordem de commit: **append no Bronze → manifesto → log**. Se o append falhar, nada é marcado como
# MAGIC ingerido e o arquivo volta na execução seguinte. Se o manifesto falhar depois do append, o Bronze
# MAGIC (segunda trava) impede a reingestão.

# COMMAND ----------

def status_arquivo(linhas_extraidas: int, gravado: bool) -> str:
    if gravado:
        return "INGERIDO"
    return "VAZIO" if linhas_extraidas == 0 else "JA_NO_BRONZE"


def ingerir_tabela(
    nome: str, df: DataFrame | None, caminho_bronze: str, arquivos: list[str], falhas: list[dict],
) -> tuple[dict, list[dict]]:
    """`arquivos` = caminhos da origem RAW pedidos ao `01`; os que falharam na extração
    (`falhas`) ficam de fora do manifesto e são retentados na próxima execução."""
    resultado = {
        "tabela": nome, "status": "OK", "linhas_gravadas": 0, "arquivos_ingeridos": [],
        "arquivos_extraidos": 0, "erro": None,
    }
    arquivos_com_falha = {f["arquivo"] for f in falhas}
    extraidos = [a for a in arquivos if a not in arquivos_com_falha]
    resultado["arquivos_extraidos"] = len(extraidos)
    vazios = [a for a in extraidos if linhas_por_arquivo.get(a, 0) == 0]
    novas_colunas: list[str] = []
    try:
        bronze_existe = tabela_delta_existe(caminho_bronze)
        gravados: dict[str, int] = {}

        if df is None:
            print(f"  {nome}: nenhum arquivo novo extraído nesta execução.")
        else:
            df_novo = adicionar_metadados_ingestao(df)
            if bronze_existe:
                ja_ingeridos = ler_delta(caminho_bronze).select("arquivo_origem").distinct()
                df_novo = df_novo.join(ja_ingeridos, on="arquivo_origem", how="left_anti")

            por_arquivo = df_novo.groupBy("arquivo_origem").agg(F.count("*").alias("linhas")).collect()
            if por_arquivo:
                novas_colunas = colunas_novas(df_novo, caminho_bronze) if bronze_existe else []
                if novas_colunas:
                    print(f"  {nome}: schema evoluindo, coluna(s) nova(s): {novas_colunas} (mergeSchema só neste write)")
                escrever_delta(
                    df_novo, caminho_bronze, "append",
                    particionar_por="data_particao", merge_schema=bool(novas_colunas),
                )
                gravados = {linha["arquivo_origem"]: linha["linhas"] for linha in por_arquivo}
                resultado["arquivos_ingeridos"] = sorted(gravados)
                resultado["linhas_gravadas"] = sum(gravados.values())

        registrar_manifesto(nome, [
            {
                "arquivo_origem": a, **meta_landing.get(a, {}),
                "linhas_extraidas": linhas_por_arquivo.get(a, 0), "linhas_gravadas": gravados.get(a, 0),
                "status": status_arquivo(linhas_por_arquivo.get(a, 0), a in gravados),
            }
            for a in extraidos
        ])
    except Exception as e:
        resultado.update(status="FALHA", erro=str(e)[:300])

    checks = [
        check_dq("janelas_extraidas_sem_falha", "COMPLETUDE", len(arquivos), len(falhas),
                 detalhe={"arquivos": [f["arquivo"] for f in falhas]} if falhas else None),
        check_dq("janelas_com_linhas", "VOLUME", len(extraidos), len(vazios),
                 detalhe={"arquivos": vazios} if vazios else None),
        check_dq("schema_sem_coluna_nova", "TECNICA", 1, len(novas_colunas),
                 detalhe={"colunas": novas_colunas} if novas_colunas else None),
    ]
    print(
        f"  {nome}: {resultado['linhas_gravadas']} linha(s) gravada(s) em "
        f"{len(resultado['arquivos_ingeridos'])} arquivo(s) | status {resultado['status']}"
    )
    return resultado, checks


resultado_produtos, dq_produtos = ingerir_tabela(
    TABELA_PRODUTOS, df_produtos, CAMINHO_BRONZE_PRODUTOS, arquivos_produtos, falhas_produtos,
)
resultado_categorias, dq_categorias = ingerir_tabela(
    TABELA_CATEGORIAS, df_categorias, CAMINHO_BRONZE_CATEGORIAS, arquivos_categorias, falhas_categorias,
)

# COMMAND ----------

# MAGIC %md ## Validação pós-escrita

# COMMAND ----------

def validar_bronze(caminho: str, resultado: dict) -> None:
    if resultado["linhas_gravadas"] == 0:
        return
    df_bronze = ler_delta(caminho)
    print(f"=== {resultado['tabela']}: {df_bronze.count()} linhas acumuladas no Bronze ===")
    # Distribuição por partição: revela se data_particao está sendo populada de forma sã.
    df_bronze.groupBy("data_particao").count().orderBy("data_particao").show()
    if "bronze_ingested_at" in df_bronze.columns:
        # NULL aqui só é esperado em linhas anteriores à migração aditiva de schema.
        sem_auditoria = df_bronze.where(F.col("bronze_ingested_at").isNull()).count()
        print(f"  Linhas sem bronze_ingested_at (anteriores à migração de schema): {sem_auditoria}")


validar_bronze(CAMINHO_BRONZE_PRODUTOS, resultado_produtos)
validar_bronze(CAMINHO_BRONZE_CATEGORIAS, resultado_categorias)

# COMMAND ----------

# MAGIC %md ## Log, Data Quality e status final para o orquestrador (Job)
# MAGIC
# MAGIC O log é gravado **antes** de levantar a exceção de falha: uma execução que quebrou também precisa
# MAGIC aparecer em `log_pipeline`.

# COMMAND ----------

for resultado, checks, falhas in [
    (resultado_produtos, dq_produtos, falhas_produtos),
    (resultado_categorias, dq_categorias, falhas_categorias),
]:
    sem_dados = resultado["arquivos_extraidos"] == 0 and not falhas
    registrar_log(
        NOTEBOOK, "bronze", resultado["tabela"],
        "FALHA" if resultado["status"] == "FALHA" else ("SEM_DADOS" if sem_dados else "SUCESSO"),
        linhas_lidas=sum(linhas_por_arquivo.get(a, 0) for a in resultado["arquivos_ingeridos"]),
        linhas_gravadas=resultado["linhas_gravadas"], linhas_rejeitadas=0,
        arquivos_processados=len(resultado["arquivos_ingeridos"]), erro=resultado["erro"],
        detalhe={"arquivos_extraidos": resultado["arquivos_extraidos"], "janelas_nao_extraidas": len(falhas)},
    )
    registrar_dq("bronze", resultado["tabela"], checks)

resultados = {"produtos": resultado_produtos, "categorias": resultado_categorias}
alertas = [{"tipo": "janela_nao_extraida", **falha} for falha in falhas_extracao]
status_final = "FALHA" if any(r["status"] == "FALHA" for r in resultados.values()) else "SUCESSO"
resumo_execucao = {
    "notebook": NOTEBOOK,
    "run_id": RUN_ID,
    "status": status_final,
    **resultados,
    "alertas": alertas,
}
print(f"Status final: {status_final} | alertas: {len(alertas)}")

if status_final == "FALHA":
    raise RuntimeError(f"Bronze terminou com falha: {json.dumps(resumo_execucao, default=str)}")

dbutils.notebook.exit(json.dumps(resumo_execucao, default=str))