# Databricks notebook source
# MAGIC %md
# MAGIC ## 🧾 Ficha de identificação
# MAGIC
# MAGIC | Informação | Detalhe |
# MAGIC |---|---|
# MAGIC | **Notebook** | `01_conexao_adls_squad2_dupla1_Parte_Lucas` |
# MAGIC | **Camada** | Extração (pré-Bronze) |
# MAGIC | **Tabelas / saídas principais** | `df_produtos`, `df_categorias` (DataFrames em memória, consumidos pelo `03`) |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | ADLS Gen2 — container RAW, pasta `real-time-data/` (somente leitura) |
# MAGIC | **Destino** | Nenhuma escrita — expõe DataFrames em memória para o notebook `03` |
# MAGIC | **Motivo** | Ler só as janelas ainda não ingeridas, sem baixar o histórico inteiro da origem a cada execução |
# MAGIC | **Última atualização** | 07/10/2026 |
# MAGIC | **Observações** | Nunca escrever, mover ou renomear nada no container RAW — ele pertence ao produtor dos dados |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 29/09/2026 | Lucas Sousa | Criação do notebook de extração |
# MAGIC | 03/10/2026 | Lucas Sousa, Zaiden Seleme | Remoção do arquivamento (`processado/`) no container RAW — Zaiden identificou escrita indevida no RAW |
# MAGIC | 07/10/2026 | Lucas Sousa | "Já ingerido" passa a considerar também os manifestos de ingestão, além do Bronze |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 · Extração da origem RAW (ADLS Gen2)
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC Responsabilidade única: ler as janelas de `real-time-data/` ainda não ingeridas e expor, como
# MAGIC DataFrames Spark, o conteúdo bruto de cada tabela com lineage por linha (`arquivo_origem`).
# MAGIC A origem RAW (container do produtor dos dados) é somente leitura: este pipeline não escreve, move nem renomeia nada nela.
# MAGIC "Ainda não ingerido" = ausente dos **manifestos de ingestão** (`controle/ingestao/`) **e** do Bronze.
# MAGIC Não valida, não deduplica por chave e não decide qual registro é "o vigente" — isso é
# MAGIC responsabilidade da Silver (`04`).
# MAGIC
# MAGIC ## Contrato de saída (consumido pelo `03`)
# MAGIC
# MAGIC | Variável | Conteúdo |
# MAGIC |---|---|
# MAGIC | `df_produtos` / `df_categorias` | DataFrame Spark com as linhas das janelas lidas, ou `None` se não houver janela nova na origem |
# MAGIC | `arquivos_produtos` / `arquivos_categorias` | Caminhos completos na origem RAW ainda não ingeridos (já filtrados antes do download) |
# MAGIC | `meta_landing` | `{caminho: {tamanho_bytes, modificado_em}}` lidos do ADLS, para o manifesto de auditoria |
# MAGIC | `linhas_por_arquivo` | `{caminho: linhas extraídas}`, incluindo janelas vazias |
# MAGIC | `falhas_extracao` | Janelas que não puderam ser lidas (arquivo corrompido, permissão) — ficam na origem e são retentadas na próxima execução |
# MAGIC
# MAGIC ## Por que não deduplica por chave
# MAGIC
# MAGIC Bronze é log bruto, e a versão vigente de cada registro é decidida na Silver pela janela de
# MAGIC origem. Deduplicar aqui descartaria histórico antes dele chegar ao Bronze.

# COMMAND ----------

# MAGIC %run ./00_config_squad2_dupla1

# COMMAND ----------

# MAGIC %md ## Varredura da origem RAW

# COMMAND ----------

def arquivos_ja_ingeridos(tabela: str, caminho_bronze: str) -> set[str]:
    """Manifestos (barato, com auditoria) ∪ Bronze (trava final). A união também cobre o histórico
    anterior aos manifestos e uma falha entre o append no Bronze e a escrita do manifesto."""
    ja = arquivos_registrados(tabela)
    if tabela_delta_existe(caminho_bronze):
        ja |= {
            linha["arquivo_origem"]
            for linha in ler_delta(caminho_bronze).select("arquivo_origem").distinct().collect()
        }
    return ja


ingeridos_produtos = arquivos_ja_ingeridos(TABELA_PRODUTOS, CAMINHO_BRONZE_PRODUTOS)
ingeridos_categorias = arquivos_ja_ingeridos(TABELA_CATEGORIAS, CAMINHO_BRONZE_CATEGORIAS)

# processado/ fica dentro de PASTA_LANDING: arquivos movidos para lá por versões antigas do
# pipeline já foram ingeridos, e não devem ser tratados como novos.
itens_landing = {
    item.name: item
    for item in fs_raw.get_paths(path=PASTA_LANDING, recursive=True)
    if not item.is_directory and not item.name.startswith(f"{PASTA_PROCESSADO}/")
}
meta_landing = {
    nome: {
        "tamanho_bytes": item.content_length,
        "modificado_em": item.last_modified.isoformat() if item.last_modified else None,
    }
    for nome, item in itens_landing.items()
}

arquivos_produtos = sorted(
    n for n in itens_landing if n.endswith(f"{TABELA_PRODUTOS}.parquet") and n not in ingeridos_produtos
)
arquivos_categorias = sorted(
    n for n in itens_landing if n.endswith(f"{TABELA_CATEGORIAS}.parquet") and n not in ingeridos_categorias
)

print(
    f"Origem RAW: {len(arquivos_produtos)} janela(s) nova(s) de produtos e "
    f"{len(arquivos_categorias)} de categorias (ainda não ingeridas)"
)

# COMMAND ----------

# MAGIC %md ## Download em memória

# COMMAND ----------

import io

import pandas as pd


def ler_parquet_em_memoria(caminho: str) -> pd.DataFrame:
    buffer = io.BytesIO()
    fs_raw.get_file_client(caminho).download_file().readinto(buffer)
    buffer.seek(0)
    return pd.read_parquet(buffer, engine="pyarrow")


def extrair_janelas(caminhos: list[str]) -> tuple[pd.DataFrame, list[dict]]:
    """Uma janela ilegível não derruba as demais: ela segue na origem, fica fora do Bronze e
    é retentada na execução seguinte."""
    quadros, falhas = [], []
    for caminho in caminhos:
        try:
            quadro = ler_parquet_em_memoria(caminho)
        except Exception as e:
            falhas.append({"arquivo": caminho, "erro": f"{type(e).__name__}: {e}"[:200]})
            continue
        quadro["arquivo_origem"] = caminho
        quadros.append(quadro)
    return (pd.concat(quadros, ignore_index=True) if quadros else pd.DataFrame()), falhas


pdf_produtos, falhas_produtos = extrair_janelas(arquivos_produtos)
pdf_categorias, falhas_categorias = extrair_janelas(arquivos_categorias)
falhas_extracao = falhas_produtos + falhas_categorias


def contar_linhas_por_arquivo(pdf: pd.DataFrame) -> dict[str, int]:
    return {} if pdf.empty else {k: int(v) for k, v in pdf["arquivo_origem"].value_counts().items()}


# Janela sem linhas não aparece em nenhum DataFrame: sem esta contagem o manifesto não saberia
# diferenciar "vazia" de "já estava no Bronze".
linhas_por_arquivo = {
    **{a: 0 for a in arquivos_produtos + arquivos_categorias},
    **contar_linhas_por_arquivo(pdf_produtos), **contar_linhas_por_arquivo(pdf_categorias),
}

for falha in falhas_extracao:
    print(f"ALERTA: janela não extraída — {falha['arquivo']}: {falha['erro']}")

# COMMAND ----------

# MAGIC %md ## Conversão para Spark

# COMMAND ----------

def para_spark(pdf: pd.DataFrame):
    return spark.createDataFrame(pdf) if not pdf.empty else None


df_produtos = para_spark(pdf_produtos)
df_categorias = para_spark(pdf_categorias)

print(f"df_produtos: {len(pdf_produtos)} linhas | df_categorias: {len(pdf_categorias)} linhas")
