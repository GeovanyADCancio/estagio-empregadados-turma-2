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
# MAGIC | **Notebook** | `05_gold_kpis_produtos_categorias_squad2_dupla1` |
# MAGIC | **Camada** | Gold |
# MAGIC | **Tabelas / saídas principais** | 9 tabelas `gold_kpi_*`/`gold_hist_*` (Delta, append) + espelho no SQL Server, incluindo 6 tabelas `_atual` |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | Camada Silver (Delta) — `ecommerce_produtos`, `ecommerce_categorias` e quarentenas |
# MAGIC | **Destino** | Camada Gold (Delta) + espelho no SQL Server (consumido pelo Looker) |
# MAGIC | **Motivo** | Calcular os KPIs de negócio de Produtos e Categorias a cada ciclo, para dashboards e tomada de decisão |
# MAGIC | **Última atualização** | 08/10/2026 |
# MAGIC | **Observações** | O Looker deve ler as tabelas `_atual` para o estado de agora e as tabelas sem `_atual` só para evolução no tempo (têm histórico acumulado por `ciclo_id`) |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 02/10/2026 | Lucas Sousa | Criação do notebook Gold, com os 8 KPIs de Produtos e Categorias |
# MAGIC | 03/10/2026 | Lucas Sousa | Correção de contagem duplicada em `gold_kpi_qualidade_dados` (regra 5) |
# MAGIC | 07/10/2026 | Lucas Sousa | Todas as tabelas passam a `append` por `ciclo_id`; adição de `gold_kpi_rejeitados_produtos` |
# MAGIC | 08/10/2026 | Lucas Sousa | Troca das views SQL Server via `pymssql` (travava o kernel com `SIGABRT`) por tabelas `_atual` regravadas a cada ciclo |
# MAGIC | 08/10/2026 | Lucas Sousa | KPIs de margem (`preco_custo`), condicionais à coluna existir na Silver |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 · Camada Gold — KPIs de Produtos e Categorias
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC Lê a Silver (`ecommerce_produtos`, `ecommerce_categorias`), recalcula os KPIs de negócio a cada
# MAGIC ciclo, grava a camada Gold em Delta (**append por ciclo**) e espelha o resultado no SQL Server.
# MAGIC
# MAGIC ## KPIs gerados
# MAGIC
# MAGIC Todas as tabelas são **append**: cada execução acrescenta o seu ciclo (`ciclo_id`) e nunca reescreve os anteriores.
# MAGIC
# MAGIC | Tabela Gold | Granularidade por ciclo | Decisão que apoia |
# MAGIC |---|---|---|
# MAGIC | `gold_kpi_catalogo_resumo` | 1 linha | Saúde do catálogo: ativos, preço, cobertura de categorias, concentração, frescor e alertas |
# MAGIC | `gold_kpi_categoria_performance` | 1 por categoria | Onde está o valor do catálogo e quais categorias exigem ação (classe de valor + diagnóstico) |
# MAGIC | `gold_kpi_categoria_raiz` | 1 por categoria raiz | Quais departamentos concentram sortimento e valor |
# MAGIC | `gold_kpi_faixa_preco` | raiz × faixa de preço | Posicionamento de preço do catálogo por departamento |
# MAGIC | `gold_kpi_qualidade_dados` | tabela × regra | Que dado ruim está tirando SKUs do catálogo |
# MAGIC | `gold_kpi_rejeitados_produtos` | 1 por SKU rejeitado | **Quais** SKUs estão na quarentena, com preço rejeitado, motivo e se ainda há versão válida na Silver |
# MAGIC | `gold_hist_catalogo_resumo` | 1 linha | Série histórica do resumo (mesmo conteúdo de `gold_kpi_catalogo_resumo`, preserva o histórico anterior à mudança para append) |
# MAGIC | `gold_hist_categoria_share` | categoria | O mix de categorias está mudando |
# MAGIC | `gold_hist_qualidade_dados` | tabela × regra | A qualidade dos dados melhora ou piora (mesmo conteúdo de `gold_kpi_qualidade_dados`) |
# MAGIC
# MAGIC ### Como o BI lê "o ciclo atual"
# MAGIC
# MAGIC Como as tabelas `gold_kpi_*` acumulam ciclos, somar sem filtrar conta o mesmo SKU várias vezes. No SQL Server existem
# MAGIC seis tabelas `<tabela>_atual` (ex.: `gold_kpi_catalogo_resumo_atual`), regravadas por este próprio notebook a
# MAGIC cada ciclo com só o ciclo corrente: **o Looker deve conectar nas tabelas `_atual` para o estado atual e nas
# MAGIC tabelas `gold_kpi_*`/`gold_hist_*` para a evolução no tempo**. No Delta, o equivalente é
# MAGIC `where ciclo_id = (select max(ciclo_id) ...)`.
# MAGIC
# MAGIC ## KPIs de margem (`preco_custo`, condicional)
# MAGIC
# MAGIC `preco_custo` foi criada pelo gestor em `ecommerce_produtos` em 08/10. Enquanto a coluna não existir na
# MAGIC Silver, estes campos ficam de fora do ciclo (sem quebrar o pipeline); assim que aparecer, entram sozinhos
# MAGIC via evolução de schema (`mergeSchema`), sem precisar reexecutar nada manualmente.
# MAGIC
# MAGIC | Onde | Campos novos |
# MAGIC |---|---|
# MAGIC | `gold_kpi_categoria_performance` | `qtd_skus_ativos_com_custo`, `pct_skus_ativos_com_custo`, `custo_total_ativo`, `valor_lista_ativo_com_custo`, `margem_valor_ativo`, `margem_pct_media_ativos`, `ranking_margem`, diagnóstico `MARGEM_NEGATIVA` |
# MAGIC | `gold_kpi_categoria_raiz` | os mesmos campos agregados por departamento |
# MAGIC | `gold_kpi_catalogo_resumo` | `skus_ativos_com_custo_informado`, `pct_skus_ativos_com_custo_informado`, `custo_total_ativo`, `margem_valor_ativo_total`, `margem_pct_media_ativos`, `skus_margem_negativa_ou_zero`, `alerta_margem_negativa`, `delta_margem_valor_ativo_total`, `delta_margem_pct_media_ativos` |
# MAGIC | `gold_hist_categoria_share` | `margem_valor_ativo`, `margem_pct_media_ativos` (evolução por categoria) |
# MAGIC
# MAGIC Margem é calculada só sobre SKUs **ativos com custo informado** — SKU sem `preco_custo` não entra na
# MAGIC conta (nem no numerador nem no denominador), para não subestimar a margem média artificialmente.
# MAGIC
# MAGIC ## Regras da planilha oficial refletidas na Gold
# MAGIC
# MAGIC - **Produtos, regra 4** — `delta_skus` (SKUs novos desde o ciclo anterior) e `alerta_novos_skus` (> 50 pode ser carga de teste).
# MAGIC - **Produtos, regra 5** — `skus_ativos_preco_invalido` e `alerta_preco_ativo_invalido` (SKU ativo com preço ≤ 0 retido na quarentena).
# MAGIC - **Categorias, regra 2** — `delta_categorias_raiz` e `alerta_categorias_raiz_mudou` (nº de categorias raiz mudou entre ciclos).
# MAGIC
# MAGIC ## Definições
# MAGIC
# MAGIC - **Valor de lista** = soma de `preco_lista`. Mede a exposição do catálogo; não é receita realizada (vendas não estão no escopo Produtos/Categorias).
# MAGIC - **Classe de valor no catálogo** (`classe_valor_catalogo`) — classe A reúne as categorias que compõem os
# MAGIC   primeiros 80% do **valor de lista ativo** (exposição, não venda); B até 95%; C o restante. **Não é a
# MAGIC   curva ABC clássica de giro/consumo** (essa é calculada sobre histórico de vendas ou saída de estoque, dado
# MAGIC   que não existe no escopo Produtos/Categorias). Uma categoria com poucos SKUs caros pode aparecer como A
# MAGIC   sem vender nada; é um indicador de onde o valor está exposto no catálogo, não de onde está a demanda.
# MAGIC - **HHI** = Σ (share % de valor por categoria)². Vai de 0 a 10.000; acima de 2.500 indica concentração alta.
# MAGIC - **Diagnóstico por categoria** — `SEM_SKU`, `SEM_OFERTA_ATIVA`, `CLASSE_A_COM_INATIVOS`, `PRECO_HETEROGENEO` ou `SAUDAVEL`; limites parametrizados na célula de parâmetros.
# MAGIC - **Shares** são calculados sobre SKUs com categoria válida; SKUs órfãos aparecem em `skus_sem_categoria_valida`.
# MAGIC - **Ciclo** — cada execução é um ciclo; `ciclo_id` (AAAAMMDDHHMMSS, UTC) é a chave de idempotência dos históricos e da sincronização com o SQL Server.
# MAGIC
# MAGIC **Persistência:** tudo em `append`, idempotente por `ciclo_id`. O Delta é a fonte da verdade; o SQL Server é um espelho
# MAGIC sincronizado a partir dele: só recebe os ciclos que ainda não tem, então uma falha de rede é compensada na execução seguinte.
# MAGIC
# MAGIC **Observabilidade:** ao final, o notebook grava `log_pipeline` (uma linha por tabela Gold) e `log_data_quality` (checks de
# MAGIC negócio, frescor, integridade e paridade com o SQL Server) e espelha **todo** o log do pipeline (Bronze, Silver e Gold) no SQL Server.

# COMMAND ----------

# MAGIC %run ./00_config_squad2_dupla1

# COMMAND ----------

# MAGIC %md ## Parâmetros de negócio
# MAGIC
# MAGIC Limites e janelas ficam aqui, num lugar só: mudar um critério é mudar uma constante.

# COMMAND ----------

import json
from datetime import datetime, timedelta, timezone
from functools import reduce

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

JANELA_HORAS = 24                    # janela dos KPIs temporais (atualização do catálogo, rejeições)
SLA_FRESCOR_HORAS = 24               # sem ingestão há mais que isso = catálogo defasado
CORTE_CLASSE_A, CORTE_CLASSE_B = 0.80, 0.95   # cortes de concentração de valor (Pareto), não de vendas
LIMITE_PCT_ATIVOS_CLASSE_A = 80.0    # categoria classe A com menos SKUs ativos que isso vira prioridade
LIMITE_CV_PRECO = 0.75               # desvio/média acima disso = preços heterogêneos na categoria

LIMITES_FAIXA_PRECO = [50, 100, 250, 500, 1000, 2500]   # a regra 2 da Silver limita preço a < 5000
ROTULOS_FAIXA_PRECO = [
    "até R$ 50", "R$ 50 a 100", "R$ 100 a 250", "R$ 250 a 500",
    "R$ 500 a 1.000", "R$ 1.000 a 2.500", "acima de R$ 2.500",
]
ESCOPO_CATALOGO = "TODO O CATÁLOGO"

DIAGNOSTICOS = {
    "SEM_SKU": "Categoria sem nenhum produto cadastrado: popular ou retirar da navegação.",
    "SEM_OFERTA_ATIVA": "Há produtos, mas nenhum ativo: reativar o sortimento ou descontinuar a categoria.",
    "CLASSE_A_COM_INATIVOS": "Categoria de maior valor com muitos SKUs inativos: priorizar a reativação.",
    "MARGEM_NEGATIVA": "Categoria vendendo abaixo do custo em média: revisar precificação com urgência.",
    "PRECO_HETEROGENEO": "Preços muito dispersos dentro da categoria: revisar política de preço e segmentação.",
    "SAUDAVEL": "Sem ação: oferta ativa e preços consistentes.",
}

GOLD_KPI_CATALOGO_RESUMO = "gold_kpi_catalogo_resumo"
GOLD_KPI_CATEGORIA_PERFORMANCE = "gold_kpi_categoria_performance"
GOLD_KPI_CATEGORIA_RAIZ = "gold_kpi_categoria_raiz"
GOLD_KPI_FAIXA_PRECO = "gold_kpi_faixa_preco"
GOLD_KPI_QUALIDADE_DADOS = "gold_kpi_qualidade_dados"
GOLD_KPI_REJEITADOS_PRODUTOS = "gold_kpi_rejeitados_produtos"
GOLD_HIST_CATALOGO_RESUMO = "gold_hist_catalogo_resumo"
GOLD_HIST_CATEGORIA_SHARE = "gold_hist_categoria_share"
GOLD_HIST_QUALIDADE_DADOS = "gold_hist_qualidade_dados"
NOTEBOOK = "05_gold_kpis_produtos_categorias_squad2_dupla1"

# Tabelas com espelho "<tabela>_atual" no SQL Server (regravado por gravar_tabelas_atual()).
GOLD_KPIS_COM_VIEW_ATUAL = [
    GOLD_KPI_CATALOGO_RESUMO, GOLD_KPI_CATEGORIA_PERFORMANCE, GOLD_KPI_CATEGORIA_RAIZ,
    GOLD_KPI_FAIXA_PRECO, GOLD_KPI_QUALIDADE_DADOS, GOLD_KPI_REJEITADOS_PRODUTOS,
]

# COMMAND ----------

# MAGIC %md ## Ciclo e funções auxiliares
# MAGIC
# MAGIC O instante do ciclo é fixado uma vez, sem microssegundos: o SQL Server (DATETIME) arredonda
# MAGIC frações de segundo, então a chave de sincronização (`ciclo_id`) é um inteiro, não um timestamp.

# COMMAND ----------

ciclo_ts = datetime.now(timezone.utc).replace(microsecond=0)
ciclo_id = int(ciclo_ts.strftime("%Y%m%d%H%M%S"))
janela_inicio = ciclo_ts - timedelta(hours=JANELA_HORAS)


def carimbar_ciclo(df: DataFrame) -> DataFrame:
    return df.select(F.lit(ciclo_id).cast("long").alias("ciclo_id"), F.lit(ciclo_ts).alias("ciclo_ts"), "*")


def razao(numerador, denominador):
    """Divisão segura: o Serverless roda em modo ANSI e levanta erro em divisão por zero."""
    den = denominador.cast("double")
    return F.when(den != 0, numerador.cast("double") / den)


def pct(numerador, denominador):
    return F.round(100 * razao(numerador, denominador), 2)


def pct_py(numerador, denominador):
    return round(100 * numerador / denominador, 2) if denominador else None


def construir_faixas(preco):
    """Retorna (ordem, rótulo) da faixa de preço; a ordem permite ordenar corretamente no BI."""
    ordem, rotulo = None, None
    for posicao, limite in enumerate(LIMITES_FAIXA_PRECO, start=1):
        condicao = preco < limite
        ordem = F.when(condicao, posicao) if ordem is None else ordem.when(condicao, posicao)
        rotulo = (
            F.when(condicao, ROTULOS_FAIXA_PRECO[posicao - 1]) if rotulo is None
            else rotulo.when(condicao, ROTULOS_FAIXA_PRECO[posicao - 1])
        )
    return ordem.otherwise(len(ROTULOS_FAIXA_PRECO)), rotulo.otherwise(ROTULOS_FAIXA_PRECO[-1])


resultados_gold: dict[str, dict] = {}


def gravar_delta_gold(nome: str, df: DataFrame) -> None:
    """Append idempotente por ciclo_id. Delta é o ponto de commit da Gold: falhou aqui, o notebook
    falha. O schema só evolui quando há coluna nova de verdade (mergeSchema deliberado)."""
    caminho = caminho_gold(nome)
    existe = tabela_delta_existe(caminho)
    if existe and ler_delta(caminho).where(F.col("ciclo_id") == ciclo_id).limit(1).count() > 0:
        print(f"  {nome}: ciclo {ciclo_id} já registrado — append ignorado.")
    else:
        novas_colunas = colunas_novas(df, caminho) if existe else []
        escrever_delta(df, caminho, "append", merge_schema=bool(novas_colunas))

    atual = ler_delta(caminho)
    linhas_ciclo = atual.where(F.col("ciclo_id") == ciclo_id).count()
    linhas_total = atual.count()
    resultados_gold[nome] = {
        "tabela": nome, "linhas_ciclo": linhas_ciclo, "linhas_delta": linhas_total,
        "sqlserver": "PENDENTE", "erro": None,
    }
    print(f"  {nome}: {linhas_ciclo} linha(s) no ciclo, {linhas_total} no Delta (append).")

# COMMAND ----------

# MAGIC %md ## Leitura da Silver
# MAGIC
# MAGIC A Gold depende só do contrato de colunas da Silver; se ele mudar, falha aqui, com mensagem clara.
# MAGIC
# MAGIC O preflight do SQL Server roda ANTES de computar qualquer KPI: se a conexão estiver quebrada
# MAGIC (driver, rede, credencial, certificado), é a mesma causa nas 8 tabelas — falhar aqui dá uma
# MAGIC mensagem só e clara, em vez de 8 tentativas de escrita no fim, cada uma reportando o mesmo erro.

# COMMAND ----------

if SQLSERVER_HABILITADO:
    diagnosticar_sqlserver()
    print("SQL Server: conectividade OK.")
else:
    print("SQL Server desabilitado (SQLSERVER_HABILITADO=False) — Gold validada só em Delta.")

# COMMAND ----------

for nome_silver, caminho_silver in [
    ("Silver produtos", CAMINHO_SILVER_PRODUTOS), ("Silver categorias", CAMINHO_SILVER_CATEGORIAS),
]:
    if not tabela_delta_existe(caminho_silver):
        raise RuntimeError(f"{nome_silver} não existe em {caminho_silver} — rode o notebook 04 antes.")

silver_produtos = ler_delta(CAMINHO_SILVER_PRODUTOS)
silver_categorias = ler_delta(CAMINHO_SILVER_CATEGORIAS)
exigir_colunas(silver_produtos, ["sku", "id_categoria", "preco_lista", "is_ativo", "data_ingestao"], "Silver produtos")
exigir_colunas(silver_categorias, ["id_categoria", "nome_categoria", "id_categoria_pai"], "Silver categorias")

# preco_custo (criada pelo gestor em 08/10): não é exigida — a coluna pode ainda não existir em todas
# as janelas durante o rollout. Quando ausente, os KPIs de margem ficam de fora deste ciclo (schema
# evolui sozinho via mergeSchema assim que a coluna aparecer, mesmo padrão usado em todo o pipeline).
SILVER_PRODUTOS_TEM_PRECO_CUSTO = "preco_custo" in silver_produtos.columns
if not SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    print("  preco_custo: coluna ainda não presente na Silver — KPIs de margem ficam pendentes neste ciclo.")

# COMMAND ----------

# MAGIC %md ## Hierarquia de categorias
# MAGIC
# MAGIC Resolve a categoria raiz e o nível de cada categoria (raiz = `id_categoria_pai` nulo, regra 2).
# MAGIC A tabela é pequena, então a árvore é percorrida no driver, com proteção contra pai inexistente e ciclo.

# COMMAND ----------

def resolver_hierarquia(linhas: list[tuple]) -> dict:
    """linhas: (id, nome, id_pai). Retorna {id: (id_raiz, nome_raiz, nivel, status)}.

    id_pai cast pra int explicitamente: no schema real, id_categoria é long mas id_categoria_pai
    é double (herdado do parquet de origem). Sem o cast, 'atual' vira float após o primeiro salto
    na árvore e id_categoria_raiz sai com tipo inconsistente (int nas raízes, float nas demais) —
    funciona hoje só por coerção implícita de Python (62 == 62.0 em chave de dict) e do
    spark.createDataFrame(schema=...) mais abaixo. Cast explícito remove essa dependência de
    comportamento não documentado (achado em auditoria, 02/10)."""
    por_id = {int(i): (nome, None if pai is None else int(pai)) for i, nome, pai in linhas}
    resolvido = {}
    for categoria in por_id:
        atual, nivel, visitados, status = categoria, 1, {categoria}, "OK"
        while por_id[atual][1] is not None:
            pai = por_id[atual][1]
            if pai not in por_id:
                status = "PAI_INEXISTENTE"
                break
            if pai in visitados:
                status = "CICLO"
                break
            visitados.add(pai)
            atual, nivel = pai, nivel + 1
        resolvido[categoria] = (atual, por_id[atual][0], nivel, status)
    return resolvido


linhas_categorias = [
    (r["id_categoria"], r["nome_categoria"], r["id_categoria_pai"])
    for r in silver_categorias.select("id_categoria", "nome_categoria", "id_categoria_pai").collect()
]
tipo_id = silver_categorias.schema["id_categoria"].dataType

hierarquia = spark.createDataFrame(
    [
        (i, raiz, None if nome_raiz is None else str(nome_raiz), nivel, status)
        for i, (raiz, nome_raiz, nivel, status) in resolver_hierarquia(linhas_categorias).items()
    ],
    StructType([
        StructField("id_categoria", tipo_id),
        StructField("id_categoria_raiz", tipo_id),
        StructField("nome_categoria_raiz", StringType()),
        StructField("nivel", IntegerType()),
        StructField("hierarquia_status", StringType()),
    ]),
)

dim_categoria = (
    silver_categorias.select("id_categoria", "nome_categoria", "id_categoria_pai")
    .join(hierarquia, "id_categoria", "left")
)

# COMMAND ----------

# MAGIC %md ## Base analítica — produtos × categorias
# MAGIC
# MAGIC `ativo` trata `is_ativo` nulo como inativo. SKU sem categoria válida fica com `id_categoria_raiz` nulo.

# COMMAND ----------

produtos_cat = (
    silver_produtos.select(
        "sku", "id_categoria", "data_ingestao",
        F.col("preco_lista").cast("double").alias("preco"),
        F.coalesce(F.col("is_ativo"), F.lit(False)).alias("ativo"),
        *([F.col("preco_custo").cast("double").alias("custo")] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    )
    .join(
        dim_categoria.select("id_categoria", "nome_categoria", "id_categoria_raiz", "nome_categoria_raiz"),
        "id_categoria", "left",
    )
)

preco_ativo = F.when(F.col("ativo"), F.col("preco"))   # nulo para inativo: agregações ignoram

if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    # custo pode ser nulo linha a linha mesmo com a coluna presente (rollout gradual do gestor) —
    # margem_valor/margem_pct ficam nulas nesse caso, e as agregações abaixo as ignoram.
    produtos_cat = (
        produtos_cat
        .withColumn("margem_valor", F.col("preco") - F.col("custo"))
        .withColumn("margem_pct", F.round(razao(F.col("margem_valor"), F.col("preco")) * 100, 2))
    )

# COMMAND ----------

# MAGIC %md ## KPI · Performance por categoria (`gold_kpi_categoria_performance`)
# MAGIC
# MAGIC Uma linha por categoria — inclusive as sem produto, que são o achado mais acionável.
# MAGIC Traz participação, classe de valor no catálogo e o diagnóstico com a ação sugerida.

# COMMAND ----------

ativo_com_custo = F.col("ativo") & F.col("custo").isNotNull() if SILVER_PRODUTOS_TEM_PRECO_CUSTO else None

por_categoria = (
    produtos_cat.where(F.col("id_categoria_raiz").isNotNull())
    .groupBy("id_categoria")
    .agg(
        F.count("*").alias("qtd_skus"),
        F.sum(F.when(F.col("ativo"), 1).otherwise(0)).alias("qtd_skus_ativos"),
        F.avg(preco_ativo).alias("preco_medio_ativos"),
        F.percentile_approx(preco_ativo, 0.5).alias("preco_mediano_ativos"),
        F.min(preco_ativo).alias("preco_min_ativos"),
        F.max(preco_ativo).alias("preco_max_ativos"),
        F.stddev_samp(preco_ativo).alias("desvio_padrao_preco_ativos"),
        F.sum(F.when(F.col("ativo"), F.col("preco")).otherwise(0.0)).alias("valor_lista_ativo"),
        F.sum(F.when(~F.col("ativo"), F.col("preco")).otherwise(0.0)).alias("valor_lista_inativo"),
        *([
            F.sum(F.when(ativo_com_custo, 1).otherwise(0)).alias("qtd_skus_ativos_com_custo"),
            F.sum(F.when(ativo_com_custo, F.col("custo")).otherwise(0.0)).alias("custo_total_ativo"),
            F.sum(F.when(ativo_com_custo, F.col("margem_valor")).otherwise(0.0)).alias("margem_valor_ativo"),
        ] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    )
)

base_categoria = dim_categoria.join(por_categoria, "id_categoria", "left")
colunas_zeradas = [
    ("qtd_skus", 0), ("qtd_skus_ativos", 0), ("valor_lista_ativo", 0.0), ("valor_lista_inativo", 0.0),
]
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    colunas_zeradas += [("qtd_skus_ativos_com_custo", 0), ("custo_total_ativo", 0.0), ("margem_valor_ativo", 0.0)]
for coluna, zero in colunas_zeradas:
    base_categoria = base_categoria.withColumn(coluna, F.coalesce(F.col(coluna), F.lit(zero)))

totais_categoria = base_categoria.agg(
    F.sum("qtd_skus_ativos").alias("ativos"), F.sum("valor_lista_ativo").alias("valor")
).first()
total_ativos_cat = totais_categoria["ativos"] or 0
total_valor_cat = totais_categoria["valor"] or 0.0

# orderBy sem partitionBy: ranking e acumulado são GLOBAIS (entre todas as categorias), não por
# grupo — Spark move tudo pra 1 partição pra isso (gera o aviso "No Partition Defined for Window
# operation"). Esperado e seguro na escala de categorias (dezenas a centenas de linhas); seria
# motivo de atenção numa tabela de milhões de linhas, não é o caso aqui.
ordem_valor = Window.orderBy(F.col("valor_lista_ativo").desc(), F.col("id_categoria"))
acumulado = F.sum("valor_lista_ativo").over(ordem_valor.rowsBetween(Window.unboundedPreceding, Window.currentRow))

# Diagnóstico por categoria: MARGEM_NEGATIVA só entra na árvore quando preco_custo existe. Montado à
# parte (não dentro do .withColumn() da cadeia abaixo) porque o ramo condicional em Python quebraria
# o encadeamento fluente dos .when(). margem_pct_media_ativos é NULL quando nenhum SKU ativo da
# categoria tem custo informado, e when() ignora NULL — cai pro próximo ramo normalmente.
_diagnostico_codigo_expr = F.when(F.col("qtd_skus") == 0, "SEM_SKU").when(F.col("qtd_skus_ativos") == 0, "SEM_OFERTA_ATIVA").when(
    (F.col("classe_valor_catalogo") == "A") & (F.col("pct_skus_ativos") < LIMITE_PCT_ATIVOS_CLASSE_A), "CLASSE_A_COM_INATIVOS"
)
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    _diagnostico_codigo_expr = _diagnostico_codigo_expr.when(F.col("margem_pct_media_ativos") < 0, "MARGEM_NEGATIVA")
_diagnostico_codigo_expr = _diagnostico_codigo_expr.when(F.col("coef_variacao_preco") > LIMITE_CV_PRECO, "PRECO_HETEROGENEO").otherwise("SAUDAVEL")

categoria_performance = (
    base_categoria
    .withColumn("qtd_skus_inativos", F.col("qtd_skus") - F.col("qtd_skus_ativos"))
    .withColumn("pct_skus_ativos", pct(F.col("qtd_skus_ativos"), F.col("qtd_skus")))
    .withColumn("coef_variacao_preco", F.round(razao(F.col("desvio_padrao_preco_ativos"), F.col("preco_medio_ativos")), 4))
    .withColumn("share_skus_ativos_pct", F.coalesce(pct(F.col("qtd_skus_ativos"), F.lit(total_ativos_cat)), F.lit(0.0)))
    .withColumn("share_valor_pct", F.coalesce(pct(F.col("valor_lista_ativo"), F.lit(total_valor_cat)), F.lit(0.0)))
    .withColumn("ranking_valor", F.row_number().over(ordem_valor))
    .transform(lambda df: (
        df
        .withColumn("pct_skus_ativos_com_custo", pct(F.col("qtd_skus_ativos_com_custo"), F.col("qtd_skus_ativos")))
        .withColumn("valor_lista_ativo_com_custo", F.col("custo_total_ativo") + F.col("margem_valor_ativo"))
        .withColumn("margem_pct_media_ativos", F.round(razao(F.col("margem_valor_ativo"), F.col("valor_lista_ativo_com_custo")) * 100, 2))
        .withColumn("ranking_margem", F.row_number().over(Window.orderBy(F.col("margem_valor_ativo").desc(), F.col("id_categoria"))))
    ) if SILVER_PRODUTOS_TEM_PRECO_CUSTO else df)
    # Classificação por CONCENTRAÇÃO DE VALOR DE LISTA (exposição do catálogo), não por vendas —
    # não existe histórico de pedidos/saída de estoque no escopo Produtos/Categorias, então isto
    # não é a curva ABC clássica de giro. A categoria é classe A se o valor acumulado ANTES dela
    # ainda não passou do corte: assim uma categoria que sozinha ultrapassa 80% continua sendo A.
    .withColumn("_acumulado_anterior", acumulado - F.col("valor_lista_ativo"))
    .withColumn(
        "classe_valor_catalogo",
        F.when(F.col("valor_lista_ativo") <= 0, "C")
         .when(razao(F.col("_acumulado_anterior"), F.lit(total_valor_cat)) < CORTE_CLASSE_A, "A")
         .when(razao(F.col("_acumulado_anterior"), F.lit(total_valor_cat)) < CORTE_CLASSE_B, "B")
         .otherwise("C"),
    )
    .withColumn("diagnostico_codigo", _diagnostico_codigo_expr)
    .withColumn(
        "diagnostico_acao",
        F.element_at(F.create_map(*[F.lit(x) for par in DIAGNOSTICOS.items() for x in par]), F.col("diagnostico_codigo")),
    )
)

colunas_preco = [
    "preco_medio_ativos", "preco_mediano_ativos", "preco_min_ativos", "preco_max_ativos",
    "desvio_padrao_preco_ativos", "valor_lista_ativo", "valor_lista_inativo",
]
colunas_margem = ["custo_total_ativo", "valor_lista_ativo_com_custo", "margem_valor_ativo"]
kpi_categoria_performance = carimbar_ciclo(
    categoria_performance.select(
        "id_categoria", "nome_categoria", "id_categoria_pai", "id_categoria_raiz", "nome_categoria_raiz",
        "nivel", "hierarquia_status", "qtd_skus", "qtd_skus_ativos", "qtd_skus_inativos", "pct_skus_ativos",
        *[F.round(c, 2).alias(c) for c in colunas_preco],
        "coef_variacao_preco", "share_skus_ativos_pct", "share_valor_pct", "ranking_valor",
        "classe_valor_catalogo", "diagnostico_codigo", "diagnostico_acao",
        *([
            "qtd_skus_ativos_com_custo", "pct_skus_ativos_com_custo",
            *[F.round(c, 2).alias(c) for c in colunas_margem],
            "margem_pct_media_ativos", "ranking_margem",
        ] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    )
)
gravar_delta_gold(GOLD_KPI_CATEGORIA_PERFORMANCE, kpi_categoria_performance)

# Os próximos KPIs partem do que acabou de ser gravado: um cálculo só, sem recomputar a Silver.
# A tabela acumula ciclos (append), então o filtro por ciclo_id é obrigatório: sem ele, os agregados
# abaixo somariam todos os ciclos já gravados.
perf = ler_delta(caminho_gold(GOLD_KPI_CATEGORIA_PERFORMANCE)).where(F.col("ciclo_id") == ciclo_id)

# COMMAND ----------

# MAGIC %md ## KPI · Departamentos (`gold_kpi_categoria_raiz`)
# MAGIC
# MAGIC Consolida a árvore de categorias em cada raiz. O preço médio é ponderado (valor ÷ SKUs ativos), não
# MAGIC uma média de médias; mediana e P90 vêm direto dos produtos porque não são somáveis.

# COMMAND ----------

por_raiz = perf.groupBy("id_categoria_raiz", "nome_categoria_raiz").agg(
    F.count("*").alias("qtd_categorias"),
    F.max("nivel").alias("profundidade_maxima"),
    F.sum("qtd_skus").alias("qtd_skus"),
    F.sum("qtd_skus_ativos").alias("qtd_skus_ativos"),
    F.sum("qtd_skus_inativos").alias("qtd_skus_inativos"),
    F.sum(F.when(F.col("qtd_skus_ativos") == 0, 1).otherwise(0)).alias("categorias_sem_sku_ativo"),
    F.sum("valor_lista_ativo").alias("valor_lista_ativo"),
    F.sum("valor_lista_inativo").alias("valor_lista_inativo"),
    *([
        F.sum("qtd_skus_ativos_com_custo").alias("qtd_skus_ativos_com_custo"),
        F.sum("custo_total_ativo").alias("custo_total_ativo"),
        F.sum("margem_valor_ativo").alias("margem_valor_ativo"),
    ] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
)
precos_raiz = (
    produtos_cat.where(F.col("ativo") & F.col("id_categoria_raiz").isNotNull())
    .groupBy("id_categoria_raiz")
    .agg(
        F.percentile_approx("preco", 0.5).alias("preco_mediano_ativos"),
        F.percentile_approx("preco", 0.9).alias("preco_p90_ativos"),
    )
)
ordem_raiz = Window.orderBy(F.col("valor_lista_ativo").desc(), F.col("id_categoria_raiz"))   # global — ver nota acima

kpi_categoria_raiz = carimbar_ciclo(
    por_raiz.join(precos_raiz, "id_categoria_raiz", "left")
    .withColumn("qtd_subcategorias", F.col("qtd_categorias") - 1)
    .withColumn("pct_skus_ativos", pct(F.col("qtd_skus_ativos"), F.col("qtd_skus")))
    .withColumn("pct_categorias_com_oferta", pct(F.col("qtd_categorias") - F.col("categorias_sem_sku_ativo"), F.col("qtd_categorias")))
    .withColumn("preco_medio_ativos", F.round(razao(F.col("valor_lista_ativo"), F.col("qtd_skus_ativos")), 2))
    .withColumn("share_skus_ativos_pct", F.coalesce(pct(F.col("qtd_skus_ativos"), F.lit(total_ativos_cat)), F.lit(0.0)))
    .withColumn("share_valor_pct", F.coalesce(pct(F.col("valor_lista_ativo"), F.lit(total_valor_cat)), F.lit(0.0)))
    .withColumn("ranking_valor", F.row_number().over(ordem_raiz))
    .transform(lambda df: (
        df
        .withColumn("pct_skus_ativos_com_custo", pct(F.col("qtd_skus_ativos_com_custo"), F.col("qtd_skus_ativos")))
        .withColumn("margem_pct_media_ativos", F.round(razao(F.col("margem_valor_ativo"), F.col("custo_total_ativo") + F.col("margem_valor_ativo")) * 100, 2))
        .withColumn("ranking_margem", F.row_number().over(Window.orderBy(F.col("margem_valor_ativo").desc(), F.col("id_categoria_raiz"))))
    ) if SILVER_PRODUTOS_TEM_PRECO_CUSTO else df)
    .select(
        "id_categoria_raiz", "nome_categoria_raiz", "qtd_categorias", "qtd_subcategorias", "profundidade_maxima",
        "qtd_skus", "qtd_skus_ativos", "qtd_skus_inativos", "pct_skus_ativos",
        "categorias_sem_sku_ativo", "pct_categorias_com_oferta",
        "preco_medio_ativos",
        F.round("preco_mediano_ativos", 2).alias("preco_mediano_ativos"),
        F.round("preco_p90_ativos", 2).alias("preco_p90_ativos"),
        F.round("valor_lista_ativo", 2).alias("valor_lista_ativo"),
        F.round("valor_lista_inativo", 2).alias("valor_lista_inativo"),
        "share_skus_ativos_pct", "share_valor_pct", "ranking_valor",
        *([
            "qtd_skus_ativos_com_custo", "pct_skus_ativos_com_custo",
            F.round("custo_total_ativo", 2).alias("custo_total_ativo"),
            F.round("margem_valor_ativo", 2).alias("margem_valor_ativo"),
            "margem_pct_media_ativos", "ranking_margem",
        ] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    )
)
gravar_delta_gold(GOLD_KPI_CATEGORIA_RAIZ, kpi_categoria_raiz)

# COMMAND ----------

# MAGIC %md ## KPI · Faixas de preço (`gold_kpi_faixa_preco`)
# MAGIC
# MAGIC Distribuição dos SKUs ativos por faixa, por departamento e para o catálogo todo
# MAGIC (`nome_categoria_raiz = 'TODO O CATÁLOGO'`, `id_categoria_raiz` nulo). Os percentuais são
# MAGIC calculados dentro de cada escopo, então somam 100% por departamento.

# COMMAND ----------

ordem_faixa, rotulo_faixa = construir_faixas(F.col("preco"))
ativos_por_faixa = (
    produtos_cat.where(F.col("ativo"))
    .withColumn("faixa_ordem", ordem_faixa)
    .withColumn("faixa_preco", rotulo_faixa)
)
metricas_faixa = [F.count("*").alias("qtd_skus_ativos"), F.sum("preco").alias("valor_lista_ativo")]

faixas_por_raiz = (
    ativos_por_faixa.where(F.col("id_categoria_raiz").isNotNull())
    .groupBy("id_categoria_raiz", "nome_categoria_raiz", "faixa_ordem", "faixa_preco")
    .agg(*metricas_faixa)
)
faixas_catalogo = (
    ativos_por_faixa.groupBy("faixa_ordem", "faixa_preco").agg(*metricas_faixa)
    .withColumn("id_categoria_raiz", F.lit(None).cast(tipo_id))
    .withColumn("nome_categoria_raiz", F.lit(ESCOPO_CATALOGO))
)
faixas = faixas_por_raiz.unionByName(faixas_catalogo)

escopo = Window.partitionBy(F.coalesce(F.col("id_categoria_raiz").cast("string"), F.lit("__catalogo__")))
kpi_faixa_preco = carimbar_ciclo(
    faixas
    .withColumn("pct_skus_no_escopo", pct(F.col("qtd_skus_ativos"), F.sum("qtd_skus_ativos").over(escopo)))
    .withColumn("pct_valor_no_escopo", pct(F.col("valor_lista_ativo"), F.sum("valor_lista_ativo").over(escopo)))
    .select(
        "id_categoria_raiz", "nome_categoria_raiz", "faixa_ordem", "faixa_preco", "qtd_skus_ativos",
        "pct_skus_no_escopo", F.round("valor_lista_ativo", 2).alias("valor_lista_ativo"), "pct_valor_no_escopo",
    )
)
gravar_delta_gold(GOLD_KPI_FAIXA_PRECO, kpi_faixa_preco)

# COMMAND ----------

# MAGIC %md ## KPI · Qualidade dos dados (`gold_kpi_qualidade_dados`)
# MAGIC
# MAGIC Lê a quarentena da Silver. Cada linha é **uma chave × uma regra técnica violada** — grau
# MAGIC aditivo: `SUM(qtd_rejeicoes)` soma certo, sem truque, mesmo agrupando por regra ou por tabela.
# MAGIC `qtd_chaves_sem_versao_valida` conta os SKUs/categorias que **ainda não têm** versão válida na
# MAGIC Silver, isto é, o que está faltando no catálogo.
# MAGIC
# MAGIC A regra 5 da planilha (SKU ativo com preço ≤ 0) **não vira linha aqui** — é um sinal de negócio
# MAGIC exclusivo, já coberto por `skus_ativos_preco_invalido` em `gold_kpi_catalogo_resumo` (e seu
# MAGIC histórico em `gold_hist_catalogo_resumo`). Injetá-la como linha extra nesta tabela inflaria
# MAGIC `SUM(qtd_rejeicoes)` para qualquer SKU que fosse "ativo" e "preço inválido" ao mesmo tempo —
# MAGIC achado em auditoria (02/10), corrigido removendo a duplicação em vez de documentá-la.

# COMMAND ----------

SCHEMA_QUALIDADE = (
    "tabela STRING, regra STRING, qtd_rejeicoes LONG, qtd_chaves_afetadas LONG, "
    "qtd_chaves_sem_versao_valida LONG, qtd_rejeicoes_janela LONG"
)


def agregar_quarentena(tabela: str, caminho: str, chave: str, silver: DataFrame) -> tuple[DataFrame | None, dict]:
    resumo = {"chaves_sem_versao_valida": 0, "ativos_preco_invalido": 0, "rejeicoes_janela": 0}
    if not tabela_delta_existe(caminho):
        return None, resumo

    q = ler_delta(caminho)
    exigir_colunas(q, [chave, "data_rejeicao"], f"Quarentena {tabela}")
    if "motivo_rejeicao" not in q.columns:   # linhas gravadas antes de a Silver registrar o motivo
        q = q.withColumn("motivo_rejeicao", F.lit(None).cast("string"))

    motivo = (
        F.when(F.col("motivo_rejeicao").isNull() | (F.col("motivo_rejeicao") == ""), F.lit("motivo_nao_registrado"))
         .otherwise(F.col("motivo_rejeicao"))
    )
    # Uma linha por (chave, regra técnica violada) — nada injetado por cima (ver nota da regra 5
    # no cabeçalho da seção). Grau aditivo: SUM(qtd_rejeicoes) soma certo em qualquer agrupamento.
    visoes = q.withColumn("regra", F.explode(F.split(motivo, ",")))

    chaves_silver = silver.select(chave).distinct()
    sem_versao = visoes.join(chaves_silver, chave, "left_anti")
    metricas = visoes.groupBy("regra").agg(
        F.count("*").alias("qtd_rejeicoes"),
        F.count_distinct(chave).alias("qtd_chaves_afetadas"),
        F.sum(F.when(F.col("data_rejeicao") >= F.lit(janela_inicio), 1).otherwise(0)).alias("qtd_rejeicoes_janela"),
    )
    pendentes = sem_versao.groupBy("regra").agg(F.count_distinct(chave).alias("qtd_chaves_sem_versao_valida"))
    por_regra = (
        metricas.join(pendentes, "regra", "left")
        .withColumn("qtd_chaves_sem_versao_valida", F.coalesce(F.col("qtd_chaves_sem_versao_valida"), F.lit(0)))
        .withColumn("tabela", F.lit(tabela))
        .select("tabela", "regra", "qtd_rejeicoes", "qtd_chaves_afetadas", "qtd_chaves_sem_versao_valida", "qtd_rejeicoes_janela")
    )

    q_sem_versao = q.join(chaves_silver, chave, "left_anti")
    resumo["chaves_sem_versao_valida"] = q_sem_versao.agg(F.count_distinct(chave)).first()[0]
    resumo["rejeicoes_janela"] = q.where(F.col("data_rejeicao") >= F.lit(janela_inicio)).count()

    # Regra 5 (negócio): calculada só pro resumo executivo (gold_kpi_catalogo_resumo), nunca
    # injetada em "visoes" acima — ver nota da regra 5 no cabeçalho da seção.
    if {"is_ativo", "preco_lista"} <= set(q.columns):
        preco_invalido = (F.col("is_ativo") == True) & (F.col("preco_lista").cast("double") <= 0)
        resumo["ativos_preco_invalido"] = q_sem_versao.where(preco_invalido).agg(F.count_distinct(chave)).first()[0]
    return por_regra, resumo


qualidade_produtos, resumo_quarentena_produtos = agregar_quarentena(
    "ecommerce_produtos", CAMINHO_SILVER_REJEITADOS_PRODUTOS, "sku", silver_produtos
)
qualidade_categorias, _ = agregar_quarentena(
    "ecommerce_categorias", CAMINHO_SILVER_REJEITADOS_CATEGORIAS, "id_categoria", silver_categorias
)

partes_qualidade = [df for df in (qualidade_produtos, qualidade_categorias) if df is not None]
qualidade = reduce(DataFrame.unionByName, partes_qualidade) if partes_qualidade else spark.createDataFrame([], SCHEMA_QUALIDADE)

kpi_qualidade_dados = carimbar_ciclo(qualidade)
gravar_delta_gold(GOLD_KPI_QUALIDADE_DADOS, kpi_qualidade_dados)

# COMMAND ----------

# MAGIC %md ## KPI · Relação de produtos rejeitados (`gold_kpi_rejeitados_produtos`)
# MAGIC
# MAGIC A lista que o gestor ou o cliente pede quando quer saber **quais** SKUs estão fora do catálogo, e não só quantos
# MAGIC (isso é `gold_kpi_qualidade_dados`). Uma linha por SKU, com a **rejeição mais recente** dele na quarentena da Silver,
# MAGIC o preço rejeitado, o motivo e o preço vigente na Silver.
# MAGIC
# MAGIC `tem_versao_valida_na_silver` responde "o produto sumiu do catálogo ou ficou com o último preço bom?":
# MAGIC `true` = a Silver preserva uma versão válida (o BI mostra o preço vigente); `false` = o SKU está fora do catálogo.
# MAGIC
# MAGIC Foto completa por ciclo (append). A quarentena é acumulativa, então a lista inclui SKUs rejeitados em ciclos anteriores
# MAGIC que depois ganharam versão válida (`tem_versao_valida_na_silver = true`). SKU com chave nula não tem identidade para
# MAGIC agrupar: cada linha de chave nula é mantida.

# COMMAND ----------

def montar_rejeitados_produtos() -> DataFrame | None:
    if not tabela_delta_existe(CAMINHO_SILVER_REJEITADOS_PRODUTOS):
        return None
    q = ler_delta(CAMINHO_SILVER_REJEITADOS_PRODUTOS)
    exigir_colunas(
        q, ["sku", "id_categoria", "preco_lista", "is_ativo", "arquivo_origem", "data_ingestao", "data_rejeicao"],
        "Quarentena produtos",
    )
    if "motivo_rejeicao" not in q.columns:
        q = q.withColumn("motivo_rejeicao", F.lit(None).cast("string"))

    mais_recente = Window.partitionBy("sku").orderBy(F.col("data_ingestao").desc(), F.col("data_rejeicao").desc())
    ultimas = (
        q.where(F.col("sku").isNotNull()).withColumn("_r", F.row_number().over(mais_recente)).where(F.col("_r") == 1).drop("_r")
        .unionByName(q.where(F.col("sku").isNull()))
    )
    vigente = silver_produtos.select("sku", F.col("preco_lista").cast("double").alias("preco_vigente_silver"))
    return (
        ultimas.join(vigente, "sku", "left")
        .select(
            "sku", "id_categoria", F.col("preco_lista").cast("double").alias("preco_rejeitado"), "is_ativo",
            "motivo_rejeicao", "arquivo_origem", "data_ingestao", "data_rejeicao", "preco_vigente_silver",
            F.col("preco_vigente_silver").isNotNull().alias("tem_versao_valida_na_silver"),
        )
    )


rejeitados_produtos = montar_rejeitados_produtos()
if rejeitados_produtos is None:
    print(f"  {GOLD_KPI_REJEITADOS_PRODUTOS}: sem quarentena de produtos — tabela não gerada neste ciclo.")
else:
    gravar_delta_gold(GOLD_KPI_REJEITADOS_PRODUTOS, carimbar_ciclo(rejeitados_produtos))

# COMMAND ----------

# MAGIC %md ## Notificação de quarentena (Teams e e-mail — ambos opcionais)
# MAGIC
# MAGIC Dispara quando há rejeição de produto dentro da janela de `JANELA_HORAS` (`rejeicoes_janela`, já
# MAGIC calculado na agregação da quarentena acima — nenhum cálculo novo). Os dois canais são
# MAGIC **independentes**: a falta ou a falha de um nunca impede o outro, e nenhum dos dois bloqueia o
# MAGIC notebook. O gestor pode usar Teams, e-mail, os dois, ou nenhum — basta configurar (ou não) os
# MAGIC secrets correspondentes em `00_config` (`notificar_teams` / `enviar_email`).

# COMMAND ----------

def notificar_quarentena_gold() -> dict:
    rejeicoes_24h = resumo_quarentena_produtos["rejeicoes_janela"]
    if rejeicoes_24h == 0 or rejeitados_produtos is None:
        return {"teams": "SEM_DISPARO", "email": "SEM_DISPARO", "rejeicoes_24h": rejeicoes_24h}

    amostra = (
        rejeitados_produtos.where(F.col("data_rejeicao") >= F.lit(janela_inicio))
        .select("sku", "motivo_rejeicao", "preco_rejeitado", "tem_versao_valida_na_silver")
        .orderBy(F.col("data_rejeicao").desc())
        .limit(50)
        .collect()
    )
    titulo = f"⚠️ {rejeicoes_24h} produto(s) em quarentena nas últimas {JANELA_HORAS}h"
    resumo_curto = (
        f"O catálogo (ciclo {ciclo_id}) teve {rejeicoes_24h} rejeição(ões) de produto nas últimas "
        f"{JANELA_HORAS}h. Corrigir na origem assim que possível."
    )

    linhas_md = "\n\n".join(
        f"- **{r['sku']}** — _{r['motivo_rejeicao'] or 'motivo não registrado'}_" for r in amostra
    )
    if rejeicoes_24h > len(amostra):
        linhas_md += f"\n\n… e mais {rejeicoes_24h - len(amostra)} item(ns) nas últimas {JANELA_HORAS}h."
    ok_teams = notificar_teams(
        titulo=titulo, texto=f"{resumo_curto}\n\n{linhas_md}",
        fatos=[("Ciclo", ciclo_id), (f"Rejeições ({JANELA_HORAS}h)", rejeicoes_24h)], cor="E81123",
    )

    linhas_texto = "\n".join(f"- {r['sku']}: {r['motivo_rejeicao'] or 'motivo não registrado'}" for r in amostra)
    linhas_html = "".join(
        f"<li><b>{r['sku']}</b> — {r['motivo_rejeicao'] or 'motivo não registrado'}</li>" for r in amostra
    )
    ok_email = enviar_email(
        assunto=titulo,
        corpo_texto=f"{resumo_curto}\n\n{linhas_texto}",
        corpo_html=f"<p>{resumo_curto}</p><ul>{linhas_html}</ul>",
    )

    return {
        "teams": "OK" if ok_teams else "NAO_ENVIADO", "email": "OK" if ok_email else "NAO_ENVIADO",
        "rejeicoes_24h": rejeicoes_24h,
    }


notificacao_quarentena = notificar_quarentena_gold()
print(f"Notificação de quarentena: {notificacao_quarentena}")

# COMMAND ----------

# MAGIC %md ## KPI · Resumo executivo do catálogo (`gold_kpi_catalogo_resumo`)
# MAGIC
# MAGIC Uma linha por ciclo, com a visão que o administrador lê primeiro: tamanho e saúde do catálogo,
# MAGIC cobertura de categorias, concentração de valor, frescor dos dados e a variação contra o ciclo anterior.
# MAGIC Os alertas só disparam contra um ciclo anterior existente: no primeiro ciclo tudo é baseline.

# COMMAND ----------

def ler_ciclo_anterior() -> dict | None:
    caminho = caminho_gold(GOLD_HIST_CATALOGO_RESUMO)
    if not tabela_delta_existe(caminho):
        return None
    linha = ler_delta(caminho).where(F.col("ciclo_id") < ciclo_id).orderBy(F.col("ciclo_id").desc()).limit(1).first()
    return linha.asDict() if linha else None


produtos_agg = produtos_cat.agg(
    F.count("*").alias("total_skus"),
    F.sum(F.when(F.col("ativo"), 1).otherwise(0)).alias("skus_ativos"),
    F.avg(preco_ativo).alias("preco_medio_ativos"),
    F.percentile_approx(preco_ativo, 0.5).alias("preco_mediano_ativos"),
    F.percentile_approx(preco_ativo, 0.9).alias("preco_p90_ativos"),
    F.sum(F.when(F.col("ativo"), F.col("preco")).otherwise(0.0)).alias("valor_lista_ativo"),
    F.sum(F.when(~F.col("ativo"), F.col("preco")).otherwise(0.0)).alias("valor_lista_inativo"),
    F.sum(F.when(F.col("id_categoria_raiz").isNull(), 1).otherwise(0)).alias("skus_sem_categoria_valida"),
    F.sum(F.when(F.col("data_ingestao") >= F.lit(janela_inicio), 1).otherwise(0)).alias("skus_atualizados_janela"),
    ((F.lit(ciclo_ts).cast("long") - F.max("data_ingestao").cast("long")) / 3600).alias("horas_desde_ultima_ingestao"),
    *([
        F.sum(F.when(ativo_com_custo, 1).otherwise(0)).alias("skus_ativos_com_custo_informado"),
        F.sum(F.when(ativo_com_custo, F.col("custo")).otherwise(0.0)).alias("custo_total_ativo_informado"),
        F.sum(F.when(ativo_com_custo, F.col("margem_valor")).otherwise(0.0)).alias("margem_valor_ativo_total"),
        F.sum(F.when(ativo_com_custo & (F.col("margem_valor") <= 0), 1).otherwise(0)).alias("skus_margem_negativa_ou_zero"),
    ] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
).first()

categorias_agg = perf.agg(
    F.count("*").alias("total_categorias"),
    F.sum(F.when(F.col("id_categoria_pai").isNull(), 1).otherwise(0)).alias("categorias_raiz"),
    F.sum(F.when(F.col("qtd_skus_ativos") > 0, 1).otherwise(0)).alias("categorias_com_sku_ativo"),
    F.sum(F.when(F.col("hierarquia_status") != "OK", 1).otherwise(0)).alias("categorias_hierarquia_invalida"),
    F.sum(F.col("share_valor_pct") * F.col("share_valor_pct")).alias("indice_hhi_valor"),
).first()
pct_valor_top3 = perf.where(F.col("ranking_valor") <= 3).agg(F.sum("share_valor_pct")).first()[0]

anterior = ler_ciclo_anterior()


def variacao(atual, anterior_valor):
    return None if anterior is None or atual is None or anterior_valor is None else atual - anterior_valor


total_skus = produtos_agg["total_skus"] or 0
skus_ativos = produtos_agg["skus_ativos"] or 0
total_categorias = categorias_agg["total_categorias"] or 0
categorias_com_oferta = categorias_agg["categorias_com_sku_ativo"] or 0
categorias_raiz = categorias_agg["categorias_raiz"] or 0
valor_ativo = produtos_agg["valor_lista_ativo"] or 0.0
preco_medio = produtos_agg["preco_medio_ativos"]
horas_defasagem = produtos_agg["horas_desde_ultima_ingestao"]
ativos_preco_invalido = resumo_quarentena_produtos["ativos_preco_invalido"]
sem_versao_valida = resumo_quarentena_produtos["chaves_sem_versao_valida"]

delta_skus = variacao(total_skus, anterior and anterior["total_skus"])
delta_categorias_raiz = variacao(categorias_raiz, anterior and anterior["categorias_raiz"])
delta_preco_pct = (
    None if anterior is None or not anterior["preco_medio_ativos"] or preco_medio is None
    else round(100 * (preco_medio - anterior["preco_medio_ativos"]) / anterior["preco_medio_ativos"], 2)
)

# Margem (extra, fora da planilha, pedido do gestor em 08/10): só calculada quando preco_custo existe
# na Silver. "anterior" pode vir de um ciclo anterior à criação da coluna — .get() evita KeyError.
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    skus_ativos_com_custo = produtos_agg["skus_ativos_com_custo_informado"] or 0
    custo_total_ativo = produtos_agg["custo_total_ativo_informado"] or 0.0
    margem_valor_ativo_total = produtos_agg["margem_valor_ativo_total"] or 0.0
    valor_lista_ativo_com_custo = custo_total_ativo + margem_valor_ativo_total
    margem_pct_media_ativos = pct_py(margem_valor_ativo_total, valor_lista_ativo_com_custo) if valor_lista_ativo_com_custo else None
    skus_margem_negativa_ou_zero = produtos_agg["skus_margem_negativa_ou_zero"] or 0
    anterior_margem = (anterior or {}).get("margem_valor_ativo_total")
    anterior_margem_pct = (anterior or {}).get("margem_pct_media_ativos")
    delta_margem_valor_ativo_total = variacao(margem_valor_ativo_total, anterior_margem)
    delta_margem_pct_media_ativos = (
        None if anterior is None or anterior_margem_pct is None or margem_pct_media_ativos is None
        else round(margem_pct_media_ativos - anterior_margem_pct, 2)
    )

CAMPOS_RESUMO = [
    ("ciclo_id", "BIGINT"), ("ciclo_ts", "TIMESTAMP"), ("janela_horas", "INT"),
    ("total_skus", "BIGINT"), ("skus_ativos", "BIGINT"), ("skus_inativos", "BIGINT"), ("pct_skus_ativos", "DOUBLE"),
    ("preco_medio_ativos", "DOUBLE"), ("preco_mediano_ativos", "DOUBLE"), ("preco_p90_ativos", "DOUBLE"),
    ("valor_lista_ativo", "DOUBLE"), ("valor_lista_inativo", "DOUBLE"),
    ("total_categorias", "BIGINT"), ("categorias_raiz", "BIGINT"), ("categorias_com_sku_ativo", "BIGINT"),
    ("categorias_sem_sku_ativo", "BIGINT"), ("pct_categorias_com_oferta", "DOUBLE"),
    ("skus_sem_categoria_valida", "BIGINT"), ("categorias_hierarquia_invalida", "BIGINT"),
    ("pct_valor_top3_categorias", "DOUBLE"), ("indice_hhi_valor", "DOUBLE"),
    ("skus_atualizados_janela", "BIGINT"), ("horas_desde_ultima_ingestao", "DOUBLE"),
    ("skus_sem_versao_valida", "BIGINT"), ("skus_ativos_preco_invalido", "BIGINT"),
    ("rejeicoes_janela", "BIGINT"), ("taxa_rejeicao_produtos_pct", "DOUBLE"),
    ("delta_skus", "BIGINT"), ("delta_skus_ativos", "BIGINT"), ("delta_valor_lista_ativo", "DOUBLE"),
    ("delta_preco_medio_ativos_pct", "DOUBLE"), ("delta_categorias_raiz", "BIGINT"),
    ("alerta_novos_skus", "BOOLEAN"), ("alerta_categorias_raiz_mudou", "BOOLEAN"),
    ("alerta_preco_ativo_invalido", "BOOLEAN"), ("alerta_catalogo_defasado", "BOOLEAN"),
]
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    CAMPOS_RESUMO += [
        ("skus_ativos_com_custo_informado", "BIGINT"), ("pct_skus_ativos_com_custo_informado", "DOUBLE"),
        ("custo_total_ativo", "DOUBLE"), ("margem_valor_ativo_total", "DOUBLE"), ("margem_pct_media_ativos", "DOUBLE"),
        ("skus_margem_negativa_ou_zero", "BIGINT"), ("alerta_margem_negativa", "BOOLEAN"),
        ("delta_margem_valor_ativo_total", "DOUBLE"), ("delta_margem_pct_media_ativos", "DOUBLE"),
    ]


def arredondar(valor):
    return None if valor is None else round(float(valor), 2)


resumo_catalogo = {
    "ciclo_id": ciclo_id, "ciclo_ts": ciclo_ts, "janela_horas": JANELA_HORAS,
    "total_skus": total_skus, "skus_ativos": skus_ativos, "skus_inativos": total_skus - skus_ativos,
    "pct_skus_ativos": pct_py(skus_ativos, total_skus),
    "preco_medio_ativos": arredondar(preco_medio),
    "preco_mediano_ativos": arredondar(produtos_agg["preco_mediano_ativos"]),
    "preco_p90_ativos": arredondar(produtos_agg["preco_p90_ativos"]),
    "valor_lista_ativo": arredondar(valor_ativo),
    "valor_lista_inativo": arredondar(produtos_agg["valor_lista_inativo"] or 0.0),
    "total_categorias": total_categorias, "categorias_raiz": categorias_raiz,
    "categorias_com_sku_ativo": categorias_com_oferta,
    "categorias_sem_sku_ativo": total_categorias - categorias_com_oferta,
    "pct_categorias_com_oferta": pct_py(categorias_com_oferta, total_categorias),
    "skus_sem_categoria_valida": produtos_agg["skus_sem_categoria_valida"] or 0,
    "categorias_hierarquia_invalida": categorias_agg["categorias_hierarquia_invalida"] or 0,
    "pct_valor_top3_categorias": arredondar(pct_valor_top3),
    "indice_hhi_valor": arredondar(categorias_agg["indice_hhi_valor"]),
    "skus_atualizados_janela": produtos_agg["skus_atualizados_janela"] or 0,
    "horas_desde_ultima_ingestao": arredondar(horas_defasagem),
    "skus_sem_versao_valida": sem_versao_valida,
    "skus_ativos_preco_invalido": ativos_preco_invalido,
    "rejeicoes_janela": resumo_quarentena_produtos["rejeicoes_janela"],
    "taxa_rejeicao_produtos_pct": pct_py(sem_versao_valida, total_skus + sem_versao_valida),
    "delta_skus": delta_skus,
    "delta_skus_ativos": variacao(skus_ativos, anterior and anterior["skus_ativos"]),
    "delta_valor_lista_ativo": arredondar(variacao(valor_ativo, anterior and anterior["valor_lista_ativo"])),
    "delta_preco_medio_ativos_pct": delta_preco_pct,
    "delta_categorias_raiz": delta_categorias_raiz,
    "alerta_novos_skus": delta_skus is not None and delta_skus > LIMITE_SKUS_NOVOS,
    "alerta_categorias_raiz_mudou": delta_categorias_raiz not in (None, 0),
    "alerta_preco_ativo_invalido": ativos_preco_invalido > 0,
    "alerta_catalogo_defasado": horas_defasagem is not None and horas_defasagem > SLA_FRESCOR_HORAS,
}
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    resumo_catalogo.update({
        "skus_ativos_com_custo_informado": skus_ativos_com_custo,
        "pct_skus_ativos_com_custo_informado": pct_py(skus_ativos_com_custo, skus_ativos),
        "custo_total_ativo": arredondar(custo_total_ativo),
        "margem_valor_ativo_total": arredondar(margem_valor_ativo_total),
        "margem_pct_media_ativos": margem_pct_media_ativos,
        "skus_margem_negativa_ou_zero": skus_margem_negativa_ou_zero,
        "alerta_margem_negativa": skus_margem_negativa_ou_zero > 0,
        "delta_margem_valor_ativo_total": arredondar(delta_margem_valor_ativo_total),
        "delta_margem_pct_media_ativos": delta_margem_pct_media_ativos,
    })


def normalizar(valor, tipo):
    """createDataFrame com schema exige o tipo Python exato: int não é aceito em coluna DOUBLE."""
    if valor is None:
        return None
    return {"DOUBLE": float, "BIGINT": int, "INT": int, "BOOLEAN": bool}.get(tipo, lambda v: v)(valor)


kpi_catalogo_resumo = spark.createDataFrame(
    [tuple(normalizar(resumo_catalogo[nome], tipo) for nome, tipo in CAMPOS_RESUMO)],
    ", ".join(f"{nome} {tipo}" for nome, tipo in CAMPOS_RESUMO),
)
gravar_delta_gold(GOLD_KPI_CATALOGO_RESUMO, kpi_catalogo_resumo)

# COMMAND ----------

# MAGIC %md ## Séries históricas
# MAGIC
# MAGIC Gravadas só depois de todos os KPIs do ciclo, para que uma falha no meio não deixe snapshot
# MAGIC parcial. Idempotentes por `ciclo_id`.

# COMMAND ----------

gravar_delta_gold(GOLD_HIST_CATALOGO_RESUMO, kpi_catalogo_resumo)

gravar_delta_gold(
    GOLD_HIST_CATEGORIA_SHARE,
    perf.select(
        "ciclo_id", "ciclo_ts", "id_categoria", "nome_categoria", "id_categoria_raiz", "nome_categoria_raiz",
        "qtd_skus_ativos", "valor_lista_ativo", "share_valor_pct", "preco_medio_ativos",
        *(["margem_valor_ativo", "margem_pct_media_ativos"] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    ),
)

gravar_delta_gold(GOLD_HIST_QUALIDADE_DADOS, kpi_qualidade_dados)

# COMMAND ----------

# MAGIC %md ## Espelho no SQL Server
# MAGIC
# MAGIC Todas as tabelas: só os ciclos que o SQL Server ainda não tem (`sincronizar_historico_sqlserver`).
# MAGIC Falha aqui não desfaz o Delta; a próxima execução reconcilia.

# COMMAND ----------

def espelhar_sqlserver(resultado: dict) -> None:
    nome = resultado["tabela"]
    if not SQLSERVER_HABILITADO:
        resultado["sqlserver"] = "DESABILITADO"
        return
    try:
        resultado["ciclos_enviados_sqlserver"] = sincronizar_historico_sqlserver(nome, caminho_gold(nome))
        resultado["sqlserver"] = "OK"
    except Exception as e:
        resultado.update(sqlserver="FALHA", erro=str(e)[:300])
    sufixo = f" — {resultado['erro']}" if resultado["sqlserver"] == "FALHA" else ""
    print(f"  {nome}: SQL Server {resultado['sqlserver']}{sufixo}")


for resultado in resultados_gold.values():
    espelhar_sqlserver(resultado)

# COMMAND ----------

# MAGIC %md ## Tabelas "atual" no SQL Server
# MAGIC
# MAGIC **Histórico (achado em execução, 08/10):** a primeira versão desta seção criava views (`CREATE OR ALTER
# MAGIC VIEW`) via `pymssql`, porque o conector `format("sqlserver")` do Serverless não executa DDL. O simples
# MAGIC `import pymssql` travou o kernel inteiro com `SIGABRT` — incompatibilidade binária da extensão nativa
# MAGIC (FreeTDS) com o runtime do Serverless, não uma exceção Python capturável. Revertido.
# MAGIC
# MAGIC **Solução atual:** nada de DDL. A cada ciclo, o notebook regrava (`overwrite` + `truncate`) uma tabela
# MAGIC `<tabela>_atual` com só as linhas do ciclo corrente, usando o mesmo `escrever_sqlserver` já testado e
# MAGIC usado para as demais 9 tabelas. O BI lê `<tabela>_atual` para o estado atual e `<tabela>` para a evolução.
# MAGIC Entre o truncate e o novo append a tabela fica momentaneamente vazia — mesmo comportamento que a Gold já
# MAGIC tinha quando `gold_kpi_*` era gravada em overwrite, antes da mudança para append histórico.
# MAGIC
# MAGIC O script `06_views_sqlserver_squad2_dupla1.sql` fica como alternativa opcional, para quem preferir views
# MAGIC de verdade e tiver como rodar DDL manualmente — não é mais necessário para o BI funcionar.

# COMMAND ----------

def gravar_atual_sqlserver(tabela: str) -> dict:
    nome_atual = f"{tabela}_atual"
    try:
        df_atual = ler_delta(caminho_gold(tabela)).where(F.col("ciclo_id") == ciclo_id)
        escrever_sqlserver(df_atual, nome_atual, "overwrite", truncar=True)
        return {"tabela": nome_atual, "status": "OK", "erro": None}
    except Exception as e:
        return {"tabela": nome_atual, "status": "FALHA", "erro": str(e)[:300]}


def gravar_tabelas_atual() -> dict:
    """Só tabelas gravadas neste ciclo recebem a atualização de `_atual`."""
    if not SQLSERVER_HABILITADO:
        return {"status": "DESABILITADO", "tabelas": {}}
    tabelas = [t for t in GOLD_KPIS_COM_VIEW_ATUAL if t in resultados_gold]
    resultado = {t: gravar_atual_sqlserver(t) for t in tabelas}
    falhas = {t: r["erro"] for t, r in resultado.items() if r["status"] == "FALHA"}
    status = "PARCIAL" if falhas else "OK"
    if falhas:
        print(f"ALERTA: tabelas _atual no SQL Server — falhas {falhas}. Sem elas o BI precisa filtrar ciclo_id "
              f"manualmente nas tabelas `gold_kpi_*` (ou usar 06_views_sqlserver_squad2_dupla1.sql).")
    else:
        print(f"Tabelas _atual no SQL Server: {len(tabelas)} atualizada(s).")
    return {"status": status, "tabelas": resultado}


tabelas_atual_sqlserver = gravar_tabelas_atual()

# COMMAND ----------

# MAGIC %md ## Log e Data Quality do ciclo
# MAGIC
# MAGIC Uma linha em `log_pipeline` por tabela Gold e um check em `log_data_quality` por regra de negócio,
# MAGIC mais a paridade Delta ↔ SQL Server. Depois, todo o log do pipeline (Bronze, Silver e Gold) é
# MAGIC espelhado no SQL Server por `log_id`: falha nesse espelho é alerta, nunca derruba a Gold.

# COMMAND ----------

def registrar_observabilidade_gold() -> dict:
    for r in resultados_gold.values():
        registrar_log(
            NOTEBOOK, "gold", r["tabela"],
            "FALHA" if r["sqlserver"] == "FALHA" else "SUCESSO",
            linhas_gravadas=r["linhas_ciclo"], erro=r["erro"],
            detalhe={
                "ciclo_id": ciclo_id, "linhas_delta_total": r["linhas_delta"], "sqlserver": r["sqlserver"],
                "ciclos_enviados_sqlserver": r.get("ciclos_enviados_sqlserver"),
            },
        )

    tabela = GOLD_KPI_CATALOGO_RESUMO
    espelhadas = [r for r in resultados_gold.values() if r["sqlserver"] != "DESABILITADO"]
    registrar_dq("gold", tabela, [
        check_dq("regra4_skus_novos_acima_do_limite", "NEGOCIO", total_skus,
                 delta_skus if resumo_catalogo["alerta_novos_skus"] else 0, passou=not resumo_catalogo["alerta_novos_skus"],
                 detalhe={"delta_skus": delta_skus, "limite": LIMITE_SKUS_NOVOS}),
        check_dq("regra5_ativos_preco_invalido", "NEGOCIO", total_skus + sem_versao_valida, ativos_preco_invalido),
        check_dq("regra2_categorias_raiz_estavel", "NEGOCIO", categorias_raiz, abs(delta_categorias_raiz or 0),
                 detalhe={"delta_categorias_raiz": delta_categorias_raiz}),
        check_dq("skus_com_categoria_valida", "INTEGRIDADE", total_skus, resumo_catalogo["skus_sem_categoria_valida"]),
        check_dq("hierarquia_de_categorias_valida", "INTEGRIDADE", total_categorias, resumo_catalogo["categorias_hierarquia_invalida"]),
        check_dq("catalogo_dentro_do_sla_de_frescor", "VOLUME", 1, int(resumo_catalogo["alerta_catalogo_defasado"]),
                 detalhe={"horas_desde_ultima_ingestao": resumo_catalogo["horas_desde_ultima_ingestao"], "sla_horas": SLA_FRESCOR_HORAS}),
        *([check_dq("paridade_delta_sqlserver", "INTEGRIDADE", len(espelhadas),
                    sum(r["sqlserver"] == "FALHA" for r in espelhadas), severidade_se_falhar="ERRO")] if espelhadas else []),
        *([check_dq("margem_nao_negativa", "NEGOCIO", resumo_catalogo["skus_ativos_com_custo_informado"],
                    resumo_catalogo["skus_margem_negativa_ou_zero"])] if SILVER_PRODUTOS_TEM_PRECO_CUSTO else []),
    ])

    if not SQLSERVER_HABILITADO:
        return {"status": "DESABILITADO"}
    try:
        enviados = {
            nome: sincronizar_historico_sqlserver(nome, caminho, chave="log_id")
            for nome, caminho in [(LOG_PIPELINE, CAMINHO_LOG_PIPELINE), (LOG_DATA_QUALITY, CAMINHO_LOG_DATA_QUALITY)]
        }
        return {"status": "OK", "registros_enviados": enviados}
    except Exception as e:
        print(f"ALERTA: espelho do log no SQL Server falhou: {str(e)[:200]}")
        return {"status": "FALHA", "erro": str(e)[:300]}


log_sqlserver = registrar_observabilidade_gold()
print(f"Log espelhado no SQL Server: {log_sqlserver}")

# COMMAND ----------

# MAGIC %md ## Resumo do ciclo e status para o orquestrador (Job)

# COMMAND ----------

ALERTAS = {
    "alerta_novos_skus": ("regra 4: SKUs novos acima do limite", "delta_skus"),
    "alerta_categorias_raiz_mudou": ("regra 2: nº de categorias raiz mudou", "delta_categorias_raiz"),
    "alerta_preco_ativo_invalido": ("regra 5: SKU ativo com preço <= 0 na quarentena", "skus_ativos_preco_invalido"),
    "alerta_catalogo_defasado": (f"catálogo sem ingestão há mais de {SLA_FRESCOR_HORAS}h", "horas_desde_ultima_ingestao"),
}
if SILVER_PRODUTOS_TEM_PRECO_CUSTO:
    ALERTAS["alerta_margem_negativa"] = ("margem: SKU(s) ativo(s) vendendo abaixo do custo", "skus_margem_negativa_ou_zero")
alertas_negocio = [
    {"alerta": descricao, "valor": resumo_catalogo[campo]}
    for flag, (descricao, campo) in ALERTAS.items() if resumo_catalogo[flag]
]

print(
    f"Ciclo {ciclo_id} | {total_skus} SKUs ({resumo_catalogo['pct_skus_ativos']}% ativos) | "
    f"{total_categorias} categorias ({categorias_raiz} raiz, {resumo_catalogo['categorias_sem_sku_ativo']} sem oferta ativa) | "
    f"valor de lista ativo R$ {valor_ativo:,.2f} | HHI {resumo_catalogo['indice_hhi_valor']}"
)
for alerta in alertas_negocio:
    print(f"ALERTA — {alerta['alerta']} (valor: {alerta['valor']})")

falhas = [r["tabela"] for r in resultados_gold.values() if r["sqlserver"] == "FALHA"]
status_final = "FALHA" if falhas else "SUCESSO"
resumo_execucao = {
    "notebook": NOTEBOOK,
    "run_id": RUN_ID,
    "status": status_final,
    "ciclo_id": ciclo_id,
    "tabelas": resultados_gold,
    "alertas_negocio": alertas_negocio,
    "tabelas_atual_sqlserver": tabelas_atual_sqlserver,
    "notificacao_quarentena": notificacao_quarentena,
    "log_sqlserver": log_sqlserver,
}
escrever_json_controle(CAMINHO_CONTROLE_GOLD, resumo_execucao)
print(f"Status final: {status_final}")

if status_final == "FALHA":
    raise RuntimeError(f"Gold terminou com falha no SQL Server em {falhas}: {json.dumps(resumo_execucao, default=str)}")

dbutils.notebook.exit(json.dumps(resumo_execucao, default=str))