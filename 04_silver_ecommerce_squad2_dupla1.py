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
# MAGIC | **Notebook** | `04_silver_ecommerce_squad2_dupla1` |
# MAGIC | **Camada** | Silver |
# MAGIC | **Tabelas / saídas principais** | `silver/ecommerce_produtos`, `silver/ecommerce_categorias` e suas quarentenas (`rejeitados_*`) |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | Camada Bronze (Delta), leitura incremental por watermark |
# MAGIC | **Destino** | Camada Silver (Delta) — upsert dos válidos, quarentena dos rejeitados |
# MAGIC | **Motivo** | Aplicar as regras técnicas e de negócio da planilha oficial e publicar a versão vigente de cada registro |
# MAGIC | **Última atualização** | 07/10/2026 |
# MAGIC | **Observações** | Regras vêm de `regras_estagio_engdados_squad2_.xlsx`, aba "Squad 2" — qualquer mudança na planilha exige atualizar `REGRAS_PRODUTOS`/`REGRAS_CATEGORIAS` |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 29/09/2026 | Lucas Sousa | Criação do notebook Silver |
# MAGIC | 02/10/2026 | Lucas Sousa | Correção da propagação de colunas de auditoria do Bronze (achado em auditoria) |
# MAGIC | 07/10/2026 | Lucas Sousa | Adição de `log_pipeline` e de checks de Data Quality por regra técnica e de negócio |
# MAGIC | 08/10/2026 | Lucas Sousa | Validação de `preco_custo` (regra6, condicional) e alerta de margem negativa — coluna criada pelo gestor |
# MAGIC | 08/10/2026 | Lucas Sousa | Notificação automática da quarentena no Microsoft Teams, a cada item novo rejeitado |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · Camada Silver (Delta) — validação, regras de negócio e upsert
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC Lê o Bronze de forma incremental (watermark), resolve a versão vigente de cada registro,
# MAGIC aplica as regras técnicas e de negócio da planilha oficial, publica os válidos via upsert e
# MAGIC envia os rejeitados para quarentena auditável.
# MAGIC
# MAGIC ## Regras aplicadas (fonte: `regras_estagio_engdados_squad2_.xlsx`, aba "Squad 2")
# MAGIC
# MAGIC | Tabela | Nº | Tipo | Regra | Onde está no código |
# MAGIC |---|---|---|---|---|
# MAGIC | produtos | 1 | Técnica | `sku` não nulo, `5 < len(sku) < 60` | `REGRAS_PRODUTOS["regra1_sku_tamanho"]` |
# MAGIC | produtos | 2 | Técnica | `preco_lista` não nulo, `0 < preco_lista < 5000` | `REGRAS_PRODUTOS["regra2_preco_lista_faixa"]` |
# MAGIC | produtos | 3 | Técnica | `is_ativo` não nulo **e** tipo boolean | `REGRAS_PRODUTOS["regra3_is_ativo_preenchido"]` + `CONTRATO_SCHEMA_PRODUTOS` |
# MAGIC | produtos | 6 | Técnica (nova, 08/10) | `preco_custo` não nulo e `> 0`, quando a coluna existir no Bronze | `REGRAS_PRODUTOS["regra6_preco_custo_valido"]` (condicional) |
# MAGIC | produtos | — | Negócio (alerta, fora da planilha) | SKU ativo com `preco_custo >= preco_lista` (margem negativa ou zero) | seção "KPI e alertas de negócio — produtos", `margem_negativa_ou_zero` |
# MAGIC | produtos | 4 | Negócio (KPI) | Nº de SKUs novos no lote; alerta se > 50 | seção "KPI e alertas de negócio — produtos" |
# MAGIC | produtos | 5 | Negócio (alerta) | SKU ativo com `preco_lista <= 0` | mesma seção, `ativos_preco_invalido` |
# MAGIC | categorias | 1 | Técnica | `id_categoria` e `nome_categoria` não nulos | `REGRAS_CATEGORIAS` |
# MAGIC | categorias | 2 | Negócio (alerta) | Nº de categorias raiz mudou entre ciclos | seção "KPI de negócio — categorias raiz" |
# MAGIC | — | extra | Negócio (alerta, fora da planilha) | Produto com `id_categoria` órfã — decisão do gestor (28/09): alertar, não bloquear | seção "Integridade referencial" |
# MAGIC
# MAGIC ## Decisões de arquitetura
# MAGIC
# MAGIC 1. **Upsert manual** (anti-join + overwrite) — `DeltaTable.merge()` não honra a credencial OAuth por chamada neste Serverless. Reescreve a tabela inteira a cada lote; aceitável em dimensões de milhares de linhas, reavaliar particionamento acima de milhões.
# MAGIC 2. **Versão vigente = janela de origem** (`AAAA/MM/DD/HHMMSS` extraída de `arquivo_origem`), não a data de ingestão — chegada fora de ordem nunca sobrescreve dado mais novo com dado mais antigo.
# MAGIC 3. **Watermark no JSON de controle**, só avança após o lote inteiro ser publicado. Ordem de commit: merge → quarentena → watermark; falha antes do último passo reprocessa o lote inteiro (os dois primeiros são idempotentes).
# MAGIC 4. **Quarentena auditável** — cada rejeitado carrega `motivo_rejeicao`; não é gravado duas vezes para a mesma versão do registro.
# MAGIC 5. **Schema da Silver** — colunas de auditoria exclusivas do Bronze não são propagadas; coluna nova de negócio entra por evolução aditiva e logada; coluna removida falha o notebook.
# MAGIC 6. **Log e Data Quality por `run_id`** — `log_pipeline` recebe uma linha por tabela (lidas, gravadas, rejeitadas, status, tempo, erro); `log_data_quality` recebe um check por regra da planilha (violações e % sobre as chaves únicas do lote), mais os checks de negócio (regras 2, 4, 5 e integridade referencial). As violações por regra saem do mesmo passe de agregação que classifica o lote, sem releitura.
# MAGIC 7. **Notificação da quarentena (Teams)** — a cada item novo rejeitado, um cartão vai para o webhook do Teams (`notificar_teams`, em `00_config`); falha no envio nunca derruba o pipeline, só fica registrada em `print`.
# MAGIC
# MAGIC ## Dependência
# MAGIC Herda `00_config` (configuração e utilitários). Não executa a extração (`01`).

# COMMAND ----------

# MAGIC %run ./00_config_squad2_dupla1

# COMMAND ----------

# MAGIC %md ## Funções do pipeline

# COMMAND ----------

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

# Auditoria/particionamento exclusivos do Bronze: Silver já tem data_ingestao e arquivo_origem
# como lineage. data_particao é derivada de data_ingestao só pra particionar o Bronze fisicamente
# — propagar pra Silver seria uma coluna de data redundante sem motivo (achado em auditoria, 02/10).
COLUNAS_BRONZE_NAO_PROPAGADAS = ["data_particao", "bronze_ingested_at", "bronze_source_file", "ano", "mes", "dia", "hora"]
COLUNA_CATEGORIA_PAI = "id_categoria_pai"   # categoria raiz = id_categoria_pai nulo
NOTEBOOK = "04_silver_ecommerce_squad2_dupla1"


@dataclass
class Lote:
    nome: str
    chave: str
    linhas_lidas: int = 0
    qtd_unicas: int = 0
    qtd_validos: int = 0
    qtd_rejeitados: int = 0
    violacoes_por_regra: dict = field(default_factory=dict)
    watermark_anterior: datetime | None = None
    proximo_watermark: datetime | None = None
    validos: DataFrame | None = None
    rejeitados: DataFrame | None = None


def versao_janela():
    """Janela de origem como texto AAAA/MM/DD/HHMMSS: largura fixa, logo a ordem lexicográfica
    é a cronológica. Independe do prefixo do caminho (inclusive processado/)."""
    return F.regexp_extract(F.col("arquivo_origem"), PADRAO_JANELA, 1)


def manter_versao_mais_recente(df: DataFrame, chave: str) -> DataFrame:
    """Uma linha por chave, pela janela de origem mais recente. Chave nula não participa da
    window: todos os nulos cairiam na mesma partição e só um sobreviveria, sumindo da
    quarentena sem rastro. Cada linha de chave nula segue adiante para ser rejeitada."""
    janela = Window.partitionBy(chave).orderBy(
        versao_janela().desc(), F.col("arquivo_origem").desc(), F.col("data_ingestao").desc()
    )
    com_chave = (
        df.where(F.col(chave).isNotNull())
          .withColumn("_rank", F.row_number().over(janela))
          .where(F.col("_rank") == 1)
          .drop("_rank")
    )
    return com_chave.unionByName(df.where(F.col(chave).isNull()))


def aplicar_regras(df: DataFrame, regras: dict) -> tuple[DataFrame, DataFrame, int, int, dict[str, int]]:
    """Retorna (válidos, rejeitados com motivo_rejeicao, total, qtd_válidos, violações por regra)
    em um único passe de agregação: as violações por regra alimentam o log de Data Quality sem
    custo de uma leitura extra. coalesce(..., False) garante lógica de dois valores: em Spark,
    NULL > 0 é NULL e tanto filter(NULL) quanto filter(~NULL) descartariam a linha dos dois lados."""
    def ok(expressao):
        return F.coalesce(expressao, F.lit(False))

    valido = reduce(and_, [ok(e) for e in regras.values()])
    motivo = F.concat_ws(",", *[F.when(~ok(e), F.lit(nome)) for nome, e in regras.items()])
    classificado = df.withColumn("_valido", valido).withColumn("_motivo", motivo)

    totais = classificado.agg(
        F.count("*").alias("total"), F.sum(F.col("_valido").cast("int")).alias("validos"),
        *[F.sum((~ok(e)).cast("int")).alias(f"v_{nome}") for nome, e in regras.items()],
    ).first()
    df_validos = classificado.where(F.col("_valido")).drop("_valido", "_motivo")
    df_rejeitados = (
        classificado.where(~F.col("_valido"))
        .withColumnRenamed("_motivo", "motivo_rejeicao").drop("_valido")
    )
    violacoes = {nome: totais[f"v_{nome}"] or 0 for nome in regras}
    return df_validos, df_rejeitados, totais["total"], totais["validos"] or 0, violacoes


def checks_tecnicos(lote: "Lote") -> list[dict]:
    """Um check de Data Quality por regra técnica da planilha, avaliado sobre as chaves únicas do lote."""
    return [check_dq(regra, "TECNICA", lote.qtd_unicas, n) for regra, n in lote.violacoes_por_regra.items()]


def obter_watermark(caminho_controle: str, caminho_silver: str, silver_existe: bool) -> datetime | None:
    """None = ler o Bronze inteiro. Silver ausente invalida qualquer watermark gravado: sem isso,
    recriar a Silver para reprocessar seria ignorado pelo JSON de controle. JSON legado, sem o
    campo novo, usa o MAX(data_ingestao) da própria Silver."""
    if not silver_existe:
        return None
    valor = (ler_json_controle(caminho_controle) or {}).get("watermark_max_data_ingestao_lida")
    if valor is None:
        return ler_delta(caminho_silver).agg(F.max("data_ingestao")).first()[0]
    return valor if isinstance(valor, datetime) else datetime.fromisoformat(str(valor))


def preparar_lote(
    nome: str, chave: str, caminho_bronze: str, caminho_silver: str, silver_existe: bool,
    caminho_controle: str, regras: dict, contrato_schema: dict[str, type] | None = None,
) -> Lote:
    if not tabela_delta_existe(caminho_bronze):
        raise RuntimeError(f"Bronze de {nome} não existe em {caminho_bronze} — rode o notebook 03 antes.")

    watermark = obter_watermark(caminho_controle, caminho_silver, silver_existe)
    df = ler_delta(caminho_bronze)
    if contrato_schema:
        exigir_contrato_schema(df, contrato_schema, nome)
    if watermark is not None:
        df = df.where(F.col("data_ingestao") > F.lit(watermark))
    df = df.drop(*COLUNAS_BRONZE_NAO_PROPAGADAS)

    leitura = df.agg(F.count("*").alias("linhas"), F.max("data_ingestao").alias("max_ingestao")).first()
    lote = Lote(
        nome=nome, chave=chave, linhas_lidas=leitura["linhas"], watermark_anterior=watermark,
        # O watermark avança pelo que foi LIDO, não pelo que foi aceito: lote 100% rejeitado
        # também precisa ser dado como processado.
        proximo_watermark=leitura["max_ingestao"] or watermark,
    )
    origem = f"watermark {watermark}" if watermark else "sem watermark (carga completa do Bronze)"
    if lote.linhas_lidas == 0:
        print(f"  {nome}: {origem} — nenhum registro novo no Bronze.")
        return lote

    atual = manter_versao_mais_recente(df, chave).withColumn("silver_processed_at", F.current_timestamp())
    lote.validos, lote.rejeitados, lote.qtd_unicas, lote.qtd_validos, lote.violacoes_por_regra = aplicar_regras(atual, regras)
    lote.qtd_rejeitados = lote.qtd_unicas - lote.qtd_validos
    print(
        f"  {nome}: {origem} — {lote.linhas_lidas} lida(s), {lote.qtd_unicas} única(s) por {chave}, "
        f"{lote.qtd_validos} válida(s), {lote.qtd_rejeitados} rejeitada(s)."
    )
    return lote


def merge_delta(
    df_origem: DataFrame, caminho_destino: str, chave: str, nome: str, qtd_origem: int, destino_existe: bool,
) -> dict:
    resultado = {"status": "OK", "linhas_ignoradas_versao_antiga": 0, "linhas_finais_na_tabela": None, "erro": None}
    if qtd_origem == 0:
        return resultado
    try:
        if not destino_existe:
            escrever_delta(df_origem, caminho_destino, "errorifexists")
            resultado["linhas_finais_na_tabela"] = qtd_origem
            print(f"  {nome}: Silver criada com {qtd_origem} linha(s) (primeira carga).")
            return resultado

        atual = ler_delta(caminho_destino)
        novas_colunas = colunas_novas(df_origem, caminho_destino)
        removidas = sorted(set(atual.columns) - set(df_origem.columns))
        if removidas:
            raise ValueError(f"Coluna(s) {removidas} existem na Silver e sumiram da origem — evolução não aditiva.")
        if novas_colunas:
            print(f"  {nome}: schema evoluindo, coluna(s) nova(s): {novas_colunas}")

        # Só substitui a linha existente se a versão recebida for da mesma janela ou mais nova.
        versao_atual = atual.select(chave, versao_janela().alias("_versao_atual"))
        elegiveis = (
            df_origem.join(versao_atual, on=chave, how="left")
            .where(F.col("_versao_atual").isNull() | (versao_janela() >= F.col("_versao_atual")))
            .drop("_versao_atual")
        )
        qtd_elegiveis = elegiveis.count()
        resultado["linhas_ignoradas_versao_antiga"] = qtd_origem - qtd_elegiveis
        if qtd_elegiveis == 0:
            print(f"  {nome}: {qtd_origem} linha(s) ignorada(s) — versão mais antiga que a da Silver.")
            return resultado

        preservados = atual.join(elegiveis.select(chave).distinct(), on=chave, how="left_anti")
        df_final = preservados.unionByName(elegiveis, allowMissingColumns=bool(novas_colunas))
        qtd_final = df_final.count()
        escrever_delta(df_final, caminho_destino, "overwrite", overwrite_schema=bool(novas_colunas))

        # Leitura de verificação: divergência aqui significa escrita concorrente ou parcial.
        qtd_gravada = ler_delta(caminho_destino).count()
        if qtd_gravada != qtd_final:
            raise RuntimeError(f"Divergência pós-escrita: esperado {qtd_final} linhas, encontrado {qtd_gravada}.")

        resultado["linhas_finais_na_tabela"] = qtd_final
        print(
            f"  {nome}: upsert aplicado — {qtd_elegiveis} linha(s) gravada(s), "
            f"{resultado['linhas_ignoradas_versao_antiga']} ignorada(s) por versão antiga, {qtd_final} na tabela."
        )
    except Exception as e:
        resultado.update(status="FALHA", erro=str(e)[:300])
    return resultado


LIMITE_AMOSTRA_NOTIFICACAO = 50   # linhas detalhadas na notificação; o total continua no corpo da mensagem


def gravar_rejeitados(df_rejeitados: DataFrame, caminho: str, chave: str, nome: str, qtd_rejeitados: int) -> dict:
    """Append idempotente: (chave, data_ingestao) identifica a versão do registro, então
    reprocessar o mesmo lote não duplica a quarentena. A comparação é null-safe porque a chave
    nula é um dos motivos de rejeição."""
    resultado = {"status": "OK", "linhas_quarentena_gravadas": 0, "amostra_novos": [], "erro": None}
    if qtd_rejeitados == 0:
        return resultado
    try:
        novos = df_rejeitados.withColumn("data_rejeicao", F.current_timestamp())
        existe = tabela_delta_existe(caminho)
        if existe:
            ja_gravados = ler_delta(caminho).select(chave, "data_ingestao").distinct()
            mesma_versao = (
                F.col(f"novos.{chave}").eqNullSafe(F.col(f"ja.{chave}"))
                & (F.col("novos.data_ingestao") == F.col("ja.data_ingestao"))
            )
            novos = novos.alias("novos").join(ja_gravados.alias("ja"), mesma_versao, "left_anti")

        qtd_novos = novos.count()
        if qtd_novos > 0:
            novas_colunas = colunas_novas(novos, caminho) if existe else []
            escrever_delta(novos, caminho, "append", merge_schema=bool(novas_colunas))
            # Amostra pra notificação (não é a tabela inteira): só os campos úteis pra quem vai corrigir.
            colunas_amostra = [c for c in [chave, "motivo_rejeicao", "preco_lista", "preco_custo", "is_ativo"] if c in novos.columns]
            resultado["amostra_novos"] = [linha.asDict() for linha in novos.select(*colunas_amostra).limit(LIMITE_AMOSTRA_NOTIFICACAO).collect()]
        resultado["linhas_quarentena_gravadas"] = qtd_novos
        print(f"  {nome}: {qtd_novos} rejeitado(s) em quarentena ({qtd_rejeitados - qtd_novos} já registrado(s)).")
    except Exception as e:
        resultado.update(status="FALHA", erro=str(e)[:300])
    return resultado


def publicar_lote(lote: Lote, caminho_silver: str, caminho_quarentena: str, silver_existia: bool) -> dict:
    """Merge primeiro, quarentena depois. Se o merge falhar nada é gravado e o lote inteiro é
    reprocessado na próxima execução."""
    resultado = {
        "tabela": lote.nome, "status": "OK", "linhas_lidas": lote.linhas_lidas,
        "linhas_unicas": lote.qtd_unicas, "linhas_validas": lote.qtd_validos,
        "linhas_rejeitadas": lote.qtd_rejeitados, "linhas_ignoradas_versao_antiga": 0,
        "linhas_quarentena_gravadas": 0, "amostra_novos": [], "linhas_finais_na_tabela": None, "erro": None,
    }
    if lote.linhas_lidas == 0:
        return resultado

    merge = merge_delta(lote.validos, caminho_silver, lote.chave, lote.nome, lote.qtd_validos, silver_existia)
    resultado.update(
        status=merge["status"], erro=merge["erro"],
        linhas_ignoradas_versao_antiga=merge["linhas_ignoradas_versao_antiga"],
        linhas_finais_na_tabela=merge["linhas_finais_na_tabela"],
    )
    if merge["status"] == "FALHA":
        return resultado

    quarentena = gravar_rejeitados(lote.rejeitados, caminho_quarentena, lote.chave, lote.nome, lote.qtd_rejeitados)
    resultado.update(
        status=quarentena["status"], erro=quarentena["erro"],
        linhas_quarentena_gravadas=quarentena["linhas_quarentena_gravadas"],
        amostra_novos=quarentena["amostra_novos"],
    )
    return resultado

# COMMAND ----------

# MAGIC %md ## Regras de validação (planilha oficial)
# MAGIC
# MAGIC Cada regra é uma expressão NULL-safe que começa por `isNotNull()`. O nome da regra vira
# MAGIC o `motivo_rejeicao` gravado na quarentena. Regras sobre o TIPO da coluna (ex.: regra 3,
# MAGIC `is_ativo` boolean) ficam fora deste dicionário — são contrato de schema, verificado uma
# MAGIC vez por lote em `exigir_contrato_schema`, não condição linha a linha.

# COMMAND ----------

REGRAS_PRODUTOS = {
    "regra1_sku_tamanho": (
        F.col("sku").isNotNull() & (F.length("sku") > 5) & (F.length("sku") < 60)
    ),
    "regra2_preco_lista_faixa": (
        F.col("preco_lista").isNotNull() & (F.col("preco_lista") > 0) & (F.col("preco_lista") < 5000)
    ),
    "regra3_is_ativo_preenchido": F.col("is_ativo").isNotNull(),
}

# regra6 só entra se o Bronze já tiver a coluna: `preco_custo` foi criada pelo gestor em 08/10 e pode
# não existir ainda em todas as janelas. Checar o schema do Bronze (metadado, não dispara leitura de
# dados) evita que REGRAS_PRODUTOS referencie uma coluna inexistente e quebre a validação inteira.
BRONZE_PRODUTOS_TEM_PRECO_CUSTO = (
    tabela_delta_existe(CAMINHO_BRONZE_PRODUTOS) and "preco_custo" in ler_delta(CAMINHO_BRONZE_PRODUTOS).columns
)
if BRONZE_PRODUTOS_TEM_PRECO_CUSTO:
    REGRAS_PRODUTOS["regra6_preco_custo_valido"] = (
        F.col("preco_custo").isNotNull() & (F.col("preco_custo") > 0)
    )
else:
    print("  preco_custo: coluna ainda não presente no Bronze — regra6 e KPIs de margem ficam pendentes.")

# Regra 3 também exige TIPO boolean, não só não-nulo — é checado uma vez por lote (invariante de
# schema), não linha a linha (ver exigir_contrato_schema).
CONTRATO_SCHEMA_PRODUTOS = {"is_ativo": BooleanType}

REGRAS_CATEGORIAS = {
    "regra1_id_categoria_preenchido": F.col("id_categoria").isNotNull(),
    "regra1_nome_categoria_preenchido": F.col("nome_categoria").isNotNull(),
}

# COMMAND ----------

# MAGIC %md ## Leitura incremental, versão mais recente e validação
# MAGIC
# MAGIC Existência das tabelas é capturada **antes** de qualquer escrita: ela define o watermark,
# MAGIC o KPI de SKUs novos e o modo de escrita do merge.

# COMMAND ----------

silver_produtos_existia = tabela_delta_existe(CAMINHO_SILVER_PRODUTOS)
silver_categorias_existia = tabela_delta_existe(CAMINHO_SILVER_CATEGORIAS)

lote_produtos = preparar_lote(
    "ecommerce_produtos", "sku", CAMINHO_BRONZE_PRODUTOS, CAMINHO_SILVER_PRODUTOS,
    silver_produtos_existia, CAMINHO_CONTROLE_SILVER_PRODUTOS, REGRAS_PRODUTOS,
    contrato_schema=CONTRATO_SCHEMA_PRODUTOS,
)
lote_categorias = preparar_lote(
    "ecommerce_categorias", "id_categoria", CAMINHO_BRONZE_CATEGORIAS, CAMINHO_SILVER_CATEGORIAS,
    silver_categorias_existia, CAMINHO_CONTROLE_SILVER_CATEGORIAS, REGRAS_CATEGORIAS,
)

# Checks de Data Quality do run, gravados em log_data_quality na célula final. As regras técnicas
# vêm da própria validação; as de negócio entram nas seções seguintes.
dq_produtos = checks_tecnicos(lote_produtos)
dq_categorias = checks_tecnicos(lote_categorias)

# COMMAND ----------

# MAGIC %md ## KPI e alertas de negócio — produtos (regras 4, 5 e margem)

# COMMAND ----------

if lote_produtos.linhas_lidas == 0:
    print("Sem lote de produtos: regras 4 e 5 não se aplicam.")
else:
    # Regra 4: SKUs novos = chaves ainda ausentes na Silver. Em primeira carga todo SKU é novo
    # por definição, então o alerta só faz sentido contra uma Silver pré-existente.
    if silver_produtos_existia:
        skus_existentes = ler_delta(CAMINHO_SILVER_PRODUTOS).select("sku").distinct()
        total_skus_novos = (
            lote_produtos.validos.join(F.broadcast(skus_existentes), on="sku", how="left_anti").count()
        )
    else:
        total_skus_novos = lote_produtos.qtd_validos
    print(f"KPI (regra 4) — novos SKUs no lote: {total_skus_novos}")
    alerta_regra4 = silver_produtos_existia and total_skus_novos > LIMITE_SKUS_NOVOS
    if alerta_regra4:
        print(f"ALERTA (regra 4): mais de {LIMITE_SKUS_NOVOS} SKUs novos no lote — possível carga de teste, "
              f"não crescimento orgânico do catálogo.")
    dq_produtos.append(check_dq(
        "regra4_skus_novos_no_lote", "NEGOCIO", lote_produtos.qtd_validos,
        total_skus_novos if alerta_regra4 else 0, passou=not alerta_regra4,
        detalhe={"skus_novos": total_skus_novos, "limite": LIMITE_SKUS_NOVOS, "primeira_carga": not silver_produtos_existia},
    ))

    # Regra 5: produto ATIVO com preço <= 0 indica bug de atualização de preço, não dado sujo
    # comum — por isso alerta próprio além da rejeição pela regra 2.
    ativos_preco_invalido = lote_produtos.rejeitados.where(
        (F.col("is_ativo") == True) & F.col("preco_lista").isNotNull() & (F.col("preco_lista") <= 0)
    )
    total_alerta_preco = ativos_preco_invalido.count()
    if total_alerta_preco > 0:
        print(f"ALERTA (regra 5): {total_alerta_preco} produto(s) ATIVO(S) com preco_lista <= 0 — "
              f"possível bug de atualização de preço.")
        ativos_preco_invalido.select("sku", "preco_lista", "is_ativo").show(10, truncate=False)
    dq_produtos.append(check_dq("regra5_ativo_preco_invalido", "NEGOCIO", lote_produtos.qtd_unicas, total_alerta_preco))

    # Margem (extra, fora da planilha, pedido do gestor em 08/10): SKU ATIVO com custo >= preço de
    # lista indica margem negativa ou zero. Alerta, não rejeição — pode ser promoção deliberada ou
    # erro de cadastro, e só se aplica a linhas que já passaram pelas regras 2 e 6 (preços válidos).
    if BRONZE_PRODUTOS_TEM_PRECO_CUSTO:
        margem_negativa = lote_produtos.validos.where(
            (F.col("is_ativo") == True) & (F.col("preco_custo") >= F.col("preco_lista"))
        )
        total_alerta_margem = margem_negativa.count()
        if total_alerta_margem > 0:
            print(f"ALERTA (margem): {total_alerta_margem} produto(s) ATIVO(S) com preco_custo >= preco_lista — "
                  f"margem negativa ou zero.")
            margem_negativa.select("sku", "preco_lista", "preco_custo", "is_ativo").show(10, truncate=False)
        dq_produtos.append(check_dq("margem_negativa_ou_zero", "NEGOCIO", lote_produtos.qtd_validos, total_alerta_margem))

# COMMAND ----------

# MAGIC %md ## Integridade referencial — produtos x categorias (alerta, não bloqueio)
# MAGIC
# MAGIC O universo de categorias é a Silver já publicada somada ao lote validado: o lote incremental
# MAGIC de produtos pode referenciar categorias de lotes anteriores.

# COMMAND ----------

df_silver_categorias = ler_delta(CAMINHO_SILVER_CATEGORIAS) if silver_categorias_existia else None

# Baseline da regra 2 (categorias raiz), capturado antes do merge: depois dele o valor já mudou.
raizes_antes = (
    df_silver_categorias.where(F.col(COLUNA_CATEGORIA_PAI).isNull()).count()
    if df_silver_categorias is not None and lote_categorias.linhas_lidas > 0 else None
)

if lote_produtos.linhas_lidas == 0:
    print("Sem lote de produtos: integridade referencial não se aplica.")
else:
    fontes_categorias = [
        df.select("id_categoria")
        for df in (df_silver_categorias, lote_categorias.validos) if df is not None
    ]
    if fontes_categorias:
        universo = reduce(DataFrame.unionByName, fontes_categorias).distinct()
        produtos_fk_invalida = lote_produtos.validos.join(F.broadcast(universo), on="id_categoria", how="left_anti")
    else:
        produtos_fk_invalida = lote_produtos.validos

    total_fk_invalida = produtos_fk_invalida.count()
    if total_fk_invalida > 0:
        print(f"ALERTA (não bloqueante) — integridade referencial: {total_fk_invalida} produto(s) com "
              f"id_categoria sem correspondência em ecommerce_categorias. Publicados normalmente "
              f"(decisão do gestor: monitorar, não quarentenar).")
        produtos_fk_invalida.select("sku", "id_categoria").show(10, truncate=False)
    else:
        print("Integridade referencial: nenhum produto órfão no lote.")
    dq_produtos.append(check_dq(
        "integridade_referencial_categoria", "INTEGRIDADE", lote_produtos.qtd_validos, total_fk_invalida,
    ))

# COMMAND ----------

# MAGIC %md ## Publicação (merge -> quarentena) e commit do watermark
# MAGIC
# MAGIC O watermark é o último passo: só avança para o lote que foi publicado por inteiro.

# COMMAND ----------

resultado_produtos = publicar_lote(
    lote_produtos, CAMINHO_SILVER_PRODUTOS, CAMINHO_SILVER_REJEITADOS_PRODUTOS, silver_produtos_existia
)
resultado_categorias = publicar_lote(
    lote_categorias, CAMINHO_SILVER_CATEGORIAS, CAMINHO_SILVER_REJEITADOS_CATEGORIAS, silver_categorias_existia
)

for lote, resultado, caminho_controle in [
    (lote_produtos, resultado_produtos, CAMINHO_CONTROLE_SILVER_PRODUTOS),
    (lote_categorias, resultado_categorias, CAMINHO_CONTROLE_SILVER_CATEGORIAS),
]:
    avancar = resultado["status"] == "OK"
    escrever_json_controle(caminho_controle, {
        **resultado,
        "ultima_execucao_utc": datetime.now(timezone.utc).isoformat(),
        "watermark_max_data_ingestao_lida": lote.proximo_watermark if avancar else lote.watermark_anterior,
    })

# COMMAND ----------

# MAGIC %md ## Notificação da quarentena (Microsoft Teams)
# MAGIC
# MAGIC Avisa a equipe responsável a cada execução que acrescentar item novo à quarentena — para correção
# MAGIC imediata na origem, sem esperar alguém abrir o notebook e exportar CSV manualmente. Só avisa sobre o
# MAGIC que é **novo** neste lote (`linhas_quarentena_gravadas`), nunca reavisa sobre o que já estava lá.
# MAGIC Dispara só se `status == "OK"`: se o merge ou a quarentena falharam, o lote inteiro é reprocessado
# MAGIC na próxima execução e notificar agora seria prematuro.

# COMMAND ----------

def notificar_quarentena(resultado: dict) -> None:
    novos = resultado["linhas_quarentena_gravadas"]
    if resultado["status"] != "OK" or novos == 0:
        return
    amostra = resultado["amostra_novos"]
    chave_item = next((c for c in ("sku", "id_categoria") if amostra and c in amostra[0]), "sku")
    linhas = "\n\n".join(
        f"- **{item.get(chave_item)}** — _{item.get('motivo_rejeicao') or 'motivo não registrado'}_"
        for item in amostra
    )
    if novos > len(amostra):
        linhas += f"\n\n… e mais {novos - len(amostra)} item(ns) — lista completa em `{resultado['tabela']}` da quarentena."
    notificar_teams(
        titulo=f"⚠️ {novos} item(ns) novo(s) em quarentena — {resultado['tabela']}",
        texto=(
            f"O pipeline Squad 2/Dupla 1 rejeitou **{novos}** registro(s) novo(s) de `{resultado['tabela']}` "
            f"nesta execução. Corrigir na origem assim que possível.\n\n{linhas}"
        ),
        fatos=[("Tabela", resultado["tabela"]), ("Novos rejeitados", novos), ("Run ID", RUN_ID)],
        cor="E81123",
    )


notificar_quarentena(resultado_produtos)
notificar_quarentena(resultado_categorias)

# COMMAND ----------

# MAGIC %md ## KPI de negócio — categorias raiz (regra 2)
# MAGIC
# MAGIC Compara a Silver inteira antes e depois do merge. Comparar o lote incremental com a tabela
# MAGIC acumulada geraria falso alerta sempre que o lote tivesse poucas categorias.

# COMMAND ----------

if lote_categorias.linhas_lidas == 0 or resultado_categorias["status"] != "OK":
    print("Categorias raiz: sem merge neste run — regra 2 não se aplica.")
else:
    raizes_depois = ler_delta(CAMINHO_SILVER_CATEGORIAS).where(F.col(COLUNA_CATEGORIA_PAI).isNull()).count()
    if raizes_antes is None:
        print(f"Categorias raiz: {raizes_depois} (baseline da primeira carga).")
    elif raizes_depois != raizes_antes:
        print(f"ALERTA (regra 2): nº de categorias raiz mudou de {raizes_antes} para {raizes_depois} — "
              f"checar se foi adição/remoção planejada.")
    else:
        print(f"Categorias raiz: {raizes_depois} (sem mudança).")
    if raizes_antes is not None:
        dq_categorias.append(check_dq(
            "regra2_categorias_raiz_estavel", "NEGOCIO", raizes_depois, abs(raizes_depois - raizes_antes),
            detalhe={"raizes_antes": raizes_antes, "raizes_depois": raizes_depois},
        ))

# COMMAND ----------

# MAGIC %md ## Manutenção — OPTIMIZE ZORDER (desabilitado, ver `00_config`)

# COMMAND ----------

def otimizar_tabela(caminho: str, coluna_chave: str, nome: str) -> None:
    try:
        spark.sql(f"OPTIMIZE delta.`{caminho}` ZORDER BY ({coluna_chave})")
        print(f"  {nome}: OPTIMIZE ZORDER aplicado.")
    except Exception as e:
        print(f"  {nome}: OPTIMIZE não aplicado ({str(e)[:200]}) — não bloqueante.")


if OPTIMIZE_HABILITADO:
    otimizar_tabela(CAMINHO_SILVER_PRODUTOS, "sku", "ecommerce_produtos")
    otimizar_tabela(CAMINHO_SILVER_CATEGORIAS, "id_categoria", "ecommerce_categorias")
else:
    print("OPTIMIZE desabilitado (OPTIMIZE_HABILITADO=False em 00_config).")

# COMMAND ----------

# MAGIC %md ## Log, Data Quality e status final para o orquestrador (Job)
# MAGIC
# MAGIC Considera merge e quarentena: ambos precisam estar íntegros para o lote ser dado como publicado.
# MAGIC O log é gravado **antes** de levantar a exceção de falha: uma execução que quebrou também precisa
# MAGIC aparecer em `log_pipeline`. `linhas_gravadas` = válidas efetivamente aplicadas no upsert (descontadas
# MAGIC as ignoradas por versão antiga); `linhas_rejeitadas` = chaves enviadas à quarentena.

# COMMAND ----------

for lote, resultado, checks in [
    (lote_produtos, resultado_produtos, dq_produtos),
    (lote_categorias, resultado_categorias, dq_categorias),
]:
    merge_aplicado = resultado["linhas_finais_na_tabela"] is not None
    registrar_log(
        NOTEBOOK, "silver", lote.nome,
        "FALHA" if resultado["status"] == "FALHA" else ("SEM_DADOS" if lote.linhas_lidas == 0 else "SUCESSO"),
        linhas_lidas=lote.linhas_lidas,
        linhas_gravadas=(resultado["linhas_validas"] - resultado["linhas_ignoradas_versao_antiga"]) if merge_aplicado else 0,
        linhas_rejeitadas=resultado["linhas_rejeitadas"], erro=resultado["erro"],
        detalhe={
            "linhas_unicas": resultado["linhas_unicas"], "linhas_validas": resultado["linhas_validas"],
            "ignoradas_versao_antiga": resultado["linhas_ignoradas_versao_antiga"],
            "quarentena_gravadas": resultado["linhas_quarentena_gravadas"],
            "linhas_finais_na_tabela": resultado["linhas_finais_na_tabela"],
            "watermark_anterior": lote.watermark_anterior, "proximo_watermark": lote.proximo_watermark,
        },
    )
    registrar_dq("silver", lote.nome, checks)

resultados = {"produtos": resultado_produtos, "categorias": resultado_categorias}
status_final = "FALHA" if any(r["status"] == "FALHA" for r in resultados.values()) else "SUCESSO"
resumo_execucao = {"notebook": NOTEBOOK, "run_id": RUN_ID, "status": status_final, **resultados}
print(f"Status final: {status_final}")

if status_final == "FALHA":
    raise RuntimeError(f"Silver terminou com falha: {json.dumps(resumo_execucao, default=str)}")

dbutils.notebook.exit(json.dumps(resumo_execucao, default=str))