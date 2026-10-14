# Databricks notebook source
# MAGIC %md
# MAGIC ## 🧾 Ficha de identificação
# MAGIC
# MAGIC | Informação | Detalhe |
# MAGIC |---|---|
# MAGIC | **Notebook** | `00_config_squad2_dupla1` |
# MAGIC | **Camada** | Infraestrutura (compartilhada por todo o pipeline) |
# MAGIC | **Tabelas / saídas principais** | Nenhuma tabela própria — fornece conexão, caminhos e funções utilitárias |
# MAGIC | **Squad / Dupla** | Squad 2 / Dupla 1 |
# MAGIC | **Responsáveis** | Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme |
# MAGIC | **Origem dos dados** | — (configuração, não processa dados) |
# MAGIC | **Destino** | — (configuração, não processa dados) |
# MAGIC | **Motivo** | Centralizar credenciais, caminhos do Lakehouse e utilitários Delta/SQL Server usados por todos os demais notebooks |
# MAGIC | **Última atualização** | 08/10/2026 |
# MAGIC | **Observações** | Alterar este notebook exige revalidar `01`, `03`, `04` e `05`, pois todos herdam dele via `%run` |
# MAGIC
# MAGIC ### Histórico de atualizações
# MAGIC
# MAGIC | Data | Desenvolvido por | Motivo |
# MAGIC |---|---|---|
# MAGIC | 29/09/2026 | Lucas Sousa | Criação do notebook de configuração |
# MAGIC | 02/10/2026 | Lucas Sousa | Escrita no SQL Server trocada para `format("sqlserver")` nativo; correção dos nomes de secrets esperados |
# MAGIC | 07/10/2026 | Lucas Sousa | Remoção da Landing Zone física; adição de manifesto de ingestão (`controle/ingestao/`), `log_pipeline` e `log_data_quality` |
# MAGIC | 08/10/2026 | Lucas Sousa | Remoção do `pymssql` — travava o kernel do Serverless (`SIGABRT`) — usado temporariamente para criar views no SQL Server |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 · Configuração compartilhada
# MAGIC **Squad 2 / Dupla 1** — Lucas Sousa Santos Oliveira, Zaiden Emiliano Segundo Seleme
# MAGIC
# MAGIC Camada de infraestrutura única para todo o pipeline: credenciais (Secret Scope), conectividade
# MAGIC com o ADLS Gen2, caminhos do Lakehouse, e utilitários de leitura/escrita Delta, de controle e
# MAGIC de SQL Server. Nenhum outro notebook redefine credenciais, caminhos ou helpers — todos herdam
# MAGIC daqui via `%run`.
# MAGIC
# MAGIC ## Quem depende deste notebook
# MAGIC
# MAGIC | Notebook | O que usa daqui |
# MAGIC |---|---|
# MAGIC | `01` (Extração) | Credenciais ADLS, `fs_raw`, `arquivos_registrados` (manifestos), caminhos da origem RAW |
# MAGIC | `03` (Bronze) | `escrever_delta`, `registrar_manifesto`, `registrar_log`, `registrar_dq`, caminhos Bronze |
# MAGIC | `04` (Silver) | `exigir_colunas`, `exigir_contrato_schema`, `registrar_log`, `registrar_dq`, caminhos Silver/controle |
# MAGIC | `05` (Gold) | Tudo acima + helpers de SQL Server (`escrever_sqlserver`, `sincronizar_historico_sqlserver`, ...) |
# MAGIC
# MAGIC ## Decisões de arquitetura
# MAGIC
# MAGIC - **`adls_options` acompanha todo read/write** — o Serverless bloqueia `spark.conf.set()` global.
# MAGIC - **Existência de tabela Delta é checada por metadado** (`_delta_log` via SDK), não por um `read` — mais barato e não depende de credencial Hadoop.
# MAGIC - **Upsert da Silver é manual** (anti-join + overwrite): `DeltaTable.merge()` e `OPTIMIZE` esbarram em `SparkKeyProviderException` com credencial OAuth por chamada neste Serverless. Migrar para Unity Catalog external location destrava os dois.
# MAGIC - **Escrita no SQL Server usa `format("sqlserver")` nativo**, não `format("jdbc")` — Serverless bloqueia DML via JDBC genérico (`UNSUPPORTED_DATA_SOURCE_WRITE`) e só libera formatos governados (achado em execução real, 02/10). Leitura continua via `jdbc`: `SELECT` não é DML, não é bloqueado.
# MAGIC - **A origem RAW (container do produtor dos dados) é somente leitura e não existe Landing Zone própria.** "Já ingerido" é decidido por dois registros no container do Lakehouse: o **manifesto de ingestão** (um JSON por execução e tabela, em `controle/ingestao/`) e o próprio Bronze (`arquivo_origem`). Os dois se complementam: o manifesto é barato de ler e carrega metadados de auditoria; o Bronze é a trava final de idempotência.
# MAGIC - **Observabilidade em duas tabelas Delta** (`log/log_pipeline` e `log/log_data_quality`), uma linha por execução × camada × tabela (e × check, no DQ), ligadas pelo `run_id`. A Gold as espelha no SQL Server.

# COMMAND ----------

# MAGIC %md ## Credenciais (Databricks Secret Scope)

# COMMAND ----------

import json
import smtplib
import socket
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import DataFrame, functions as F

SECRET_SCOPE = "squad2-dupla1"


def obter_secret(*chaves: str) -> str | None:
    """Tenta as grafias da chave em ordem. Só erro de permissão propaga: mascará-lo como
    'secret ausente' esconde a causa real e leva a diagnóstico errado."""
    for chave in chaves:
        try:
            valor = dbutils.secrets.get(scope=SECRET_SCOPE, key=chave)
        except Exception as e:
            if "permission" in str(e).lower():
                raise
            continue
        if valor:
            return valor.strip()
    return None


tenant_id = obter_secret("tenant-id", "tenant_id", "ADLS_TENANT_ID")
client_id = obter_secret("client-id", "client_id", "ADLS_CLIENT_ID")
client_secret = obter_secret("client-secret", "client_secret", "ADLS_CLIENT_SECRET")
storage_account = obter_secret("storage-account", "storage_account")
container = obter_secret("container", "container_name")

ausentes = [
    nome for nome, valor in [
        ("tenant_id", tenant_id), ("client_id", client_id), ("client_secret", client_secret),
        ("storage_account", storage_account), ("container", container),
    ] if not valor
]
if ausentes:
    raise ValueError(
        f"Secrets ausentes no scope '{SECRET_SCOPE}': {ausentes}. Confira o scope com "
        f"dbutils.secrets.listScopes() e as keys com dbutils.secrets.list('{SECRET_SCOPE}')."
    )

# COMMAND ----------

# MAGIC %md ## Conectividade

# COMMAND ----------

hostname = f"{storage_account}.dfs.core.windows.net"

try:
    ip_resolvido = socket.gethostbyname(hostname)
except socket.gaierror as e:
    raise RuntimeError(f"Falha na resolução DNS do endpoint do Data Lake: {e}") from e

# IP privado indica o bloqueio de rede já mapeado no Serverless desta workspace: a leitura
# falharia com ConnectionResetError mesmo com credencial correta (rede e credencial são
# camadas independentes).
if ip_resolvido.startswith("192.168."):
    print("ALERTA: o endpoint resolveu para IP privado — possível bloqueio de rede do Serverless.")

CONTAINER_LAKEHOUSE = "squad2"
CONTAINER_RAW_LANDING = container

_servico_datalake = DataLakeServiceClient(
    account_url=f"https://{hostname}",
    credential=ClientSecretCredential(tenant_id, client_id, client_secret),
)
fs_lakehouse = _servico_datalake.get_file_system_client(CONTAINER_LAKEHOUSE)
fs_raw = _servico_datalake.get_file_system_client(CONTAINER_RAW_LANDING)

adls_options = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# COMMAND ----------

# MAGIC %md ## Caminhos do Lakehouse e da origem RAW

# COMMAND ----------

RAIZ_LAKEHOUSE = "grupo1/grupo1_lucas"   # caminho isolado: não colide com o que o Zaiden grava
BASE_LAKEHOUSE = f"abfss://{CONTAINER_LAKEHOUSE}@{hostname}/{RAIZ_LAKEHOUSE}"

CAMINHO_BRONZE_PRODUTOS = f"{BASE_LAKEHOUSE}/bronze/ecommerce_produtos"
CAMINHO_BRONZE_CATEGORIAS = f"{BASE_LAKEHOUSE}/bronze/ecommerce_categorias"
CAMINHO_SILVER_PRODUTOS = f"{BASE_LAKEHOUSE}/silver/ecommerce_produtos"
CAMINHO_SILVER_CATEGORIAS = f"{BASE_LAKEHOUSE}/silver/ecommerce_categorias"
CAMINHO_SILVER_REJEITADOS_PRODUTOS = f"{BASE_LAKEHOUSE}/silver/rejeitados_ecommerce_produtos"
CAMINHO_SILVER_REJEITADOS_CATEGORIAS = f"{BASE_LAKEHOUSE}/silver/rejeitados_ecommerce_categorias"

TABELA_PRODUTOS = "ecommerce_produtos"
TABELA_CATEGORIAS = "ecommerce_categorias"

# Controle é acessado via SDK: caminhos relativos ao container do Lakehouse.
CAMINHO_CONTROLE_SILVER_PRODUTOS = f"{RAIZ_LAKEHOUSE}/controle/silver_ecommerce_produtos.json"
CAMINHO_CONTROLE_SILVER_CATEGORIAS = f"{RAIZ_LAKEHOUSE}/controle/silver_ecommerce_categorias.json"
CAMINHO_CONTROLE_GOLD = f"{RAIZ_LAKEHOUSE}/controle/gold_kpis_produtos_categorias.json"
PASTA_MANIFESTOS = f"{RAIZ_LAKEHOUSE}/controle/ingestao"   # <tabela>/<run_id>.json

# Observabilidade: Delta no Lakehouse; espelhadas no SQL Server pela Gold (mesmo nome de tabela).
LOG_PIPELINE = "log_pipeline"
LOG_DATA_QUALITY = "log_data_quality"
CAMINHO_LOG_PIPELINE = f"{BASE_LAKEHOUSE}/log/{LOG_PIPELINE}"
CAMINHO_LOG_DATA_QUALITY = f"{BASE_LAKEHOUSE}/log/{LOG_DATA_QUALITY}"

PASTA_LANDING = "real-time-data"
PASTA_PROCESSADO = f"{PASTA_LANDING}/processado"   # legado: arquivos movidos para cá por versões antigas já estão no Bronze; a extração (01) os ignora
PADRAO_JANELA = r"(\d{4}/\d{2}/\d{2}/\d{6})"       # AAAA/MM/DD/HHMMSS — ordena cronologicamente como texto


def caminho_gold(nome_tabela: str) -> str:
    return f"{BASE_LAKEHOUSE}/gold/{nome_tabela}"


# Regra 4 da planilha (KPI de SKUs novos): lote/ciclo acima disso pode ser carga de teste.
# Compartilhado porque Silver (alerta por lote) e Gold (alerta por ciclo) aplicam a mesma regra.
LIMITE_SKUS_NOVOS = 50

# Serverless + OAuth por chamada não é honrado pelo OPTIMIZE (SparkKeyProviderException).
# Habilitar apenas após a migração para Unity Catalog.
OPTIMIZE_HABILITADO = False

# SQL Server é destino exclusivo da Gold (espelho de serving dos KPIs). Desligar permite validar
# a Gold só em Delta enquanto o acesso ao banco não está liberado.
SQLSERVER_HABILITADO = True
SQLSERVER_SCHEMA = "dbo"
SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
SQLSERVER_CONFIAR_CERTIFICADO = False   # True só para servidor com certificado autoassinado

# COMMAND ----------

# MAGIC %md ## Utilitários Delta e controle

# COMMAND ----------

def tabela_delta_existe(caminho: str) -> bool:
    """Checagem por metadado (_delta_log) via SDK: não dispara job Spark nem depende de
    credencial Hadoop."""
    relativo = caminho.split(".dfs.core.windows.net/", 1)[1]
    try:
        fs_lakehouse.get_directory_client(f"{relativo}/_delta_log").get_directory_properties()
        return True
    except ResourceNotFoundError:
        return False


def ler_delta(caminho: str) -> DataFrame:
    return spark.read.format("delta").options(**adls_options).load(caminho)


def escrever_delta(
    df: DataFrame,
    caminho: str,
    modo: str,
    particionar_por: str | None = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
) -> None:
    escritor = (
        df.write.format("delta").mode(modo).options(**adls_options)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
    )
    if particionar_por:
        escritor = escritor.partitionBy(particionar_por)
    if merge_schema:
        escritor = escritor.option("mergeSchema", "true")
    if overwrite_schema:
        escritor = escritor.option("overwriteSchema", "true")
    escritor.save(caminho)


def colunas_novas(df: DataFrame, caminho: str) -> list[str]:
    """Colunas do DataFrame ausentes na tabela existente. Evolução de schema é sempre
    deliberada: quem chama liga mergeSchema/overwriteSchema só quando esta lista não é vazia."""
    return sorted(set(df.columns) - set(ler_delta(caminho).columns))


def exigir_colunas(df: DataFrame, colunas: list[str], nome: str) -> None:
    ausentes = sorted(set(colunas) - set(df.columns))
    if ausentes:
        raise ValueError(f"{nome}: coluna(s) obrigatória(s) ausente(s): {ausentes}")


def exigir_contrato_schema(df: DataFrame, contrato: dict[str, type], nome: str) -> None:
    """Falha cedo se o TIPO físico de uma coluna não bater com o contrato técnico da planilha
    (ex.: regra 3, 'is_ativo deve ser boolean'). Diferente de regras linha a linha, é um
    invariante de schema: tipo errado invalida a tabela inteira — não faz sentido "rejeitar"
    só as linhas que vieram no formato errado, porque todas vieram."""
    tipos_reais = {campo.name: type(campo.dataType) for campo in df.schema.fields}
    divergencias = [
        f"{coluna}: esperado {esperado.__name__}, obtido "
        f"{tipos_reais[coluna].__name__ if coluna in tipos_reais else 'coluna ausente'}"
        for coluna, esperado in contrato.items()
        if tipos_reais.get(coluna) is not esperado
    ]
    if divergencias:
        raise TypeError(f"{nome}: contrato de schema violado — {'; '.join(divergencias)}")


def ler_json_controle(caminho_relativo: str) -> dict | None:
    try:
        return json.loads(fs_lakehouse.get_file_client(caminho_relativo).download_file().readall())
    except ResourceNotFoundError:
        return None


def escrever_json_controle(caminho_relativo: str, dados: dict) -> None:
    conteudo = json.dumps(dados, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    fs_lakehouse.get_file_client(caminho_relativo).upload_data(conteudo, overwrite=True)

# COMMAND ----------

# MAGIC %md ## Execução, manifesto de ingestão e observabilidade
# MAGIC
# MAGIC `RUN_ID` identifica a execução em todos os registros (manifesto, log, DQ). Notebooks encadeados por
# MAGIC `%run` (01 → 03) compartilham o mesmo `RUN_ID`; cada Job run, sendo uma sessão nova, ganha um novo.
# MAGIC
# MAGIC **Manifesto de ingestão:** um JSON por execução e tabela, em `controle/ingestao/<tabela>/<run_id>.json`,
# MAGIC listando cada arquivo da origem RAW tratado, com tamanho, data de modificação no ADLS, linhas e status
# MAGIC (`INGERIDO`, `VAZIO` ou `JA_NO_BRONZE`). Um arquivo por execução (e não por arquivo de origem) mantém o
# MAGIC número de objetos pequeno e dispensa escrita concorrente sobre o mesmo JSON.

# COMMAND ----------

INICIO_EXECUCAO = datetime.now(timezone.utc)
RUN_ID = f"{INICIO_EXECUCAO:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def registrar_manifesto(tabela: str, arquivos: list[dict]) -> None:
    """Primeira escrita vence: reexecutar uma célula na mesma sessão não sobrescreve o manifesto
    com o resultado degradado de um reprocessamento (JA_NO_BRONZE). A checagem é explícita e a
    escrita usa overwrite=True, o mesmo caminho já validado em escrever_json_controle: upload com
    overwrite=False falhou com PathNotFound em pasta ainda inexistente (achado em execução, 07/10)."""
    if not arquivos:
        return
    pasta = f"{PASTA_MANIFESTOS}/{tabela}"
    cliente = fs_lakehouse.get_file_client(f"{pasta}/{RUN_ID}.json")
    try:
        cliente.get_file_properties()
        return
    except ResourceNotFoundError:
        pass
    conteudo = json.dumps({
        "run_id": RUN_ID, "tabela": tabela, "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "qtd_arquivos": len(arquivos), "arquivos": arquivos,
    }, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    try:
        cliente.upload_data(conteudo, overwrite=True)
    except ResourceNotFoundError:   # pasta pai ausente: cria e tenta de novo
        fs_lakehouse.get_directory_client(pasta).create_directory()
        cliente.upload_data(conteudo, overwrite=True)


def arquivos_registrados(tabela: str) -> set[str]:
    """Todos os arquivos de origem já tratados segundo os manifestos da tabela.
    ponytail: lê todos os manifestos a cada execução (8 em paralelo); com milhares deles, compactar
    em um snapshot periódico."""
    try:
        manifestos = [
            p.name for p in fs_lakehouse.get_paths(f"{PASTA_MANIFESTOS}/{tabela}", recursive=False)
            if p.name.endswith(".json")
        ]
    except ResourceNotFoundError:
        return set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        conteudos = list(pool.map(ler_json_controle, manifestos))
    return {a["arquivo_origem"] for c in conteudos if c for a in c["arquivos"]}


SCHEMA_LOG_PIPELINE = (
    "log_id STRING, run_id STRING, notebook STRING, camada STRING, tabela STRING, status STRING, "
    "inicio_utc TIMESTAMP, fim_utc TIMESTAMP, duracao_s DOUBLE, linhas_lidas LONG, linhas_gravadas LONG, "
    "linhas_rejeitadas LONG, arquivos_processados LONG, erro STRING, detalhe STRING"
)
SCHEMA_LOG_DQ = (
    "log_id STRING, run_id STRING, camada STRING, tabela STRING, check_nome STRING, tipo STRING, "
    "qtd_avaliadas LONG, qtd_violacoes LONG, pct_violacao DOUBLE, passou BOOLEAN, severidade STRING, "
    "detalhe STRING, registrado_em_utc TIMESTAMP"
)


def _inteiro(valor) -> int | None:
    return None if valor is None else int(valor)


def _anexar_log(caminho: str, linhas: list[tuple], schema: str, descricao: str) -> None:
    """Falha de log nunca derruba o pipeline (o dado de negócio já foi gravado), mas também nunca
    é silenciosa: aparece no output do notebook."""
    try:
        escrever_delta(spark.createDataFrame(linhas, schema), caminho, "append")
    except Exception as e:
        print(f"ALERTA: {descricao} não gravado em {caminho}: {str(e)[:200]}")


def registrar_log(
    notebook: str, camada: str, tabela: str, status: str,
    linhas_lidas=None, linhas_gravadas=None, linhas_rejeitadas=None, arquivos_processados=None,
    erro: str | None = None, detalhe: dict | None = None,
) -> None:
    """status: SUCESSO | FALHA | SEM_DADOS. Uma linha por execução × camada × tabela."""
    fim = datetime.now(timezone.utc)
    linha = (
        f"{RUN_ID}|{camada}|{tabela}", RUN_ID, notebook, camada, tabela, status,
        INICIO_EXECUCAO, fim, round((fim - INICIO_EXECUCAO).total_seconds(), 1),
        _inteiro(linhas_lidas), _inteiro(linhas_gravadas), _inteiro(linhas_rejeitadas),
        _inteiro(arquivos_processados), erro,
        json.dumps(detalhe, ensure_ascii=False, default=str) if detalhe else None,
    )
    _anexar_log(CAMINHO_LOG_PIPELINE, [linha], SCHEMA_LOG_PIPELINE, "log_pipeline")


def check_dq(
    nome: str, tipo: str, avaliadas, violacoes, passou: bool | None = None,
    severidade_se_falhar: str = "ALERTA", detalhe: dict | None = None,
) -> dict:
    """tipo: TECNICA | NEGOCIO | INTEGRIDADE | VOLUME | COMPLETUDE. Por padrão passa se não há
    violações; checks com limite (ex.: regra 4) informam `passou` explicitamente."""
    avaliadas, violacoes = int(avaliadas or 0), int(violacoes or 0)
    passou = violacoes == 0 if passou is None else passou
    return {
        "check_nome": nome, "tipo": tipo, "qtd_avaliadas": avaliadas, "qtd_violacoes": violacoes,
        "pct_violacao": round(100 * violacoes / avaliadas, 2) if avaliadas else None,
        "passou": passou, "severidade": "OK" if passou else severidade_se_falhar, "detalhe": detalhe,
    }


def registrar_dq(camada: str, tabela: str, checks: list[dict]) -> None:
    if not checks:
        return
    agora = datetime.now(timezone.utc)
    linhas = [
        (
            f"{RUN_ID}|{camada}|{tabela}|{c['check_nome']}", RUN_ID, camada, tabela, c["check_nome"], c["tipo"],
            c["qtd_avaliadas"], c["qtd_violacoes"], c["pct_violacao"], c["passou"], c["severidade"],
            json.dumps(c["detalhe"], ensure_ascii=False, default=str) if c["detalhe"] else None, agora,
        )
        for c in checks
    ]
    _anexar_log(CAMINHO_LOG_DATA_QUALITY, linhas, SCHEMA_LOG_DQ, "log_data_quality")
    reprovados = [c["check_nome"] for c in checks if not c["passou"]]
    print(f"  DQ {camada}/{tabela}: {len(checks)} check(s), {len(reprovados)} reprovado(s) {reprovados or ''}")

# COMMAND ----------

# MAGIC %md ## Notificação (Microsoft Teams)
# MAGIC
# MAGIC Alerta a equipe responsável quando a Silver rejeita algo novo, para correção imediata na origem.
# MAGIC Secret opcional (`GRAFIAS_SECRET_TEAMS`); usa `urllib` (stdlib) — sem driver nativo, pela mesma
# MAGIC razão que o `pymssql` foi removido de `05` (travava o kernel do Serverless com `SIGABRT`, 08/10).
# MAGIC Falha de rede, secret ausente ou erro HTTP nunca derrubam o pipeline: só geram um aviso impresso.

# COMMAND ----------

GRAFIAS_SECRET_TEAMS = ("teams-webhook-url", "teams_webhook_url", "TEAMS_WEBHOOK_URL")
_teams_webhook_url: str | None = None
_teams_webhook_tentado = False


def _obter_teams_webhook() -> str | None:
    global _teams_webhook_url, _teams_webhook_tentado
    if not _teams_webhook_tentado:
        _teams_webhook_url = obter_secret(*GRAFIAS_SECRET_TEAMS)
        _teams_webhook_tentado = True
    return _teams_webhook_url


def notificar_teams(titulo: str, texto: str, fatos: list[tuple[str, object]] | None = None, cor: str = "FFA500") -> bool:
    """Envia um cartão MessageCard para o Incoming Webhook do Teams. Retorna True só em sucesso
    confirmado; qualquer outro caso (secret ausente, rede, HTTP) retorna False e imprime o motivo."""
    url = _obter_teams_webhook()
    if not url:
        print(f"  Teams: secret ausente no scope '{SECRET_SCOPE}' (tente uma destas keys: "
              f"{', '.join(GRAFIAS_SECRET_TEAMS)}) — notificação não enviada.")
        return False

    payload = {
        "@type": "MessageCard", "@context": "http://schema.org/extensions",
        "summary": titulo, "themeColor": cor, "title": titulo, "text": texto,
    }
    if fatos:
        payload["sections"] = [{"facts": [{"name": nome, "value": str(valor)} for nome, valor in fatos]}]

    requisicao = urllib.request.Request(
        url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"}, method="POST",
    )
    try:
        with urllib.request.urlopen(requisicao, timeout=15) as resposta:
            if resposta.status == 200:
                return True
            print(f"  Teams: resposta inesperada do webhook (HTTP {resposta.status}).")
            return False
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"  Teams: falha ao notificar ({str(e)[:200]}) — não bloqueante, pipeline continua.")
        return False

# COMMAND ----------

# MAGIC %md ## Notificação (e-mail)
# MAGIC
# MAGIC Segundo canal, independente do Teams: o gestor usa um, o outro, os dois, ou nenhum — basta criar
# MAGIC (ou não) os secrets correspondentes. `smtplib` + `email` (stdlib), sem biblioteca nova a instalar.
# MAGIC Falha de rede, secret ausente ou erro de autenticação nunca derrubam o pipeline.

# COMMAND ----------

GRAFIAS_SECRET_EMAIL = {
    "host": ("smtp-host", "smtp_host", "SMTP_HOST"),
    "usuario": ("smtp-user", "smtp_user", "SMTP_USER"),
    "senha": ("smtp-password", "smtp_password", "SMTP_PASSWORD"),
    "destinatarios": ("smtp-destinatarios", "smtp_destinatarios", "SMTP_DESTINATARIOS"),
}
_config_email: dict | None = None
_config_email_tentado = False


def _obter_config_email() -> dict | None:
    """Lê os secrets do e-mail uma única vez. `destinatarios` aceita uma ou mais contas separadas
    por vírgula ou ponto e vírgula. Porta e remetente têm padrão, não exigem secret próprio."""
    global _config_email, _config_email_tentado
    if _config_email_tentado:
        return _config_email
    _config_email_tentado = True

    valores = {campo: obter_secret(*grafias) for campo, grafias in GRAFIAS_SECRET_EMAIL.items()}
    ausentes = [campo for campo, valor in valores.items() if not valor]
    if ausentes:
        faltando = {campo: GRAFIAS_SECRET_EMAIL[campo] for campo in ausentes}
        print(f"  E-mail: secret(s) ausente(s) no scope '{SECRET_SCOPE}' — {faltando} — notificação não enviada.")
        return None

    porta = obter_secret("smtp-port", "smtp_port", "SMTP_PORT") or "587"
    remetente = obter_secret("smtp-remetente", "smtp_remetente", "SMTP_REMETENTE") or valores["usuario"]
    _config_email = {
        "host": valores["host"], "porta": int(porta), "usuario": valores["usuario"],
        "senha": valores["senha"], "remetente": remetente,
        "destinatarios": [d.strip() for d in valores["destinatarios"].replace(";", ",").split(",") if d.strip()],
    }
    return _config_email


def enviar_email(assunto: str, corpo_texto: str, corpo_html: str | None = None) -> bool:
    """Envia por SMTP com STARTTLS (Gmail, Outlook/Office 365 e a maioria dos provedores corporativos
    aceitam esse modo na porta 587). Retorna True só em envio confirmado pelo servidor."""
    cfg = _obter_config_email()
    if not cfg:
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"], msg["From"], msg["To"] = assunto, cfg["remetente"], ", ".join(cfg["destinatarios"])
    msg.attach(MIMEText(corpo_texto, "plain", "utf-8"))
    if corpo_html:
        msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    try:
        with smtplib.SMTP(cfg["host"], cfg["porta"], timeout=15) as servidor:
            servidor.starttls()
            servidor.login(cfg["usuario"], cfg["senha"])
            servidor.sendmail(cfg["remetente"], cfg["destinatarios"], msg.as_string())
        return True
    except Exception as e:
        print(f"  E-mail: falha ao notificar ({str(e)[:200]}) — não bloqueante, pipeline continua.")
        return False

# COMMAND ----------

# MAGIC %md ## SQL Server (destino exclusivo da Gold)
# MAGIC
# MAGIC Credenciais lidas do mesmo Secret Scope, **sob demanda**: Bronze e Silver não dependem delas.
# MAGIC Chaves esperadas: `sqlserver-host`, `sqlserver-database`, `sqlserver-user`,
# MAGIC `sqlserver-password` e, opcionalmente, `sqlserver-port` (padrão 1433).

# COMMAND ----------

_config_sqlserver: dict | None = None

# Cada campo tenta estas grafias de secret, em ordem. Único lugar que define o nome esperado:
# a mensagem de erro é gerada a partir daqui, então sempre lista as grafias REAIS tentadas,
# nunca um rótulo genérico que não corresponde ao nome de secret de verdade.
GRAFIAS_SECRET_SQLSERVER = {
    "host": ("sqlserver-host", "sqlserver_host", "SQLSERVER_HOST"),
    "database": ("sqlserver-database", "sqlserver_database", "SQLSERVER_DATABASE"),
    "user": ("sqlserver-user", "sqlserver_user", "SQLSERVER_USER"),
    "password": ("sqlserver-password", "sqlserver_password", "SQLSERVER_PASSWORD"),
}


def config_sqlserver() -> dict:
    global _config_sqlserver
    if _config_sqlserver is None:
        valores = {campo: obter_secret(*grafias) for campo, grafias in GRAFIAS_SECRET_SQLSERVER.items()}
        porta = obter_secret("sqlserver-port", "sqlserver_port", "SQLSERVER_PORT") or "1433"

        ausentes = {campo: grafias for campo, grafias in GRAFIAS_SECRET_SQLSERVER.items() if not valores[campo]}
        if ausentes:
            detalhe = "; ".join(f"{campo} (crie uma destas keys: {', '.join(g)})" for campo, g in ausentes.items())
            raise ValueError(f"Secrets do SQL Server ausentes no scope '{SECRET_SCOPE}': {detalhe}.")

        confiar = "true" if SQLSERVER_CONFIAR_CERTIFICADO else "false"
        _config_sqlserver = {
            "host": valores["host"], "port": porta, "database": valores["database"],
            "user": valores["user"], "password": valores["password"],
            "encrypt": "true", "trustServerCertificate": confiar,
            # "url" só serve pro leitor genérico format("jdbc") abaixo — Serverless aceita
            # format("jdbc") pra LEITURA (SELECT não é DML). Pra escrita ver _opcoes_escrita_sqlserver().
            "url": (
                f"jdbc:sqlserver://{valores['host']}:{porta};databaseName={valores['database']};encrypt=true;"
                f"trustServerCertificate={confiar};loginTimeout=30"
            ),
        }
    return _config_sqlserver


def _opcoes_leitura_sqlserver() -> dict:
    cfg = config_sqlserver()
    return {"url": cfg["url"], "user": cfg["user"], "password": cfg["password"], "driver": SQLSERVER_DRIVER}


def _opcoes_escrita_sqlserver() -> dict:
    """Opções nomeadas (host/port/database), não uma connection string montada: é o formato que
    o conector nativo format("sqlserver") exige — ver motivo em escrever_sqlserver()."""
    cfg = config_sqlserver()
    return {
        "host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
        "user": cfg["user"], "password": cfg["password"],
        "encrypt": cfg["encrypt"], "trustServerCertificate": cfg["trustServerCertificate"],
    }


def ler_sqlserver_query(consulta: str) -> DataFrame:
    return spark.read.format("jdbc").options(**_opcoes_leitura_sqlserver()).option("query", consulta).load()


def diagnosticar_sqlserver() -> None:
    """Preflight de UMA chamada (SELECT 1), antes de computar qualquer KPI: se o SQL Server
    estiver inacessível — rede/firewall, secret errado, certificado — a falha é a MESMA em todas as
    tabelas da Gold. Sem isto, o notebook gasta o processamento de todas as tabelas pra só
    então reportar 8 vezes o mesmo erro genérico, em vez de uma mensagem clara e imediata.
    IMPORTANTE: isto testa só LEITURA (jdbc) — não pega um problema que seja exclusivo do
    caminho de ESCRITA (format("sqlserver"), ver escrever_sqlserver()); erro de escrita só
    aparece mesmo na hora de gravar, já dentro de espelhar_sqlserver()."""
    try:
        ler_sqlserver_query("SELECT 1 AS ok").collect()
    except Exception as e:
        raise RuntimeError(
            f"SQL Server inacessível (SQLSERVER_HABILITADO=True): {str(e)[:500]}. "
            f"Causas comuns: driver com.microsoft.sqlserver.jdbc.SQLServerDriver não instalado "
            f"como biblioteca do cluster; firewall/rede do Databricks até o host; secret errado "
            f"no scope '{SECRET_SCOPE}'; certificado autoassinado (ver SQLSERVER_CONFIAR_CERTIFICADO). "
            f"Para validar só o Delta enquanto isso não está resolvido, ponha SQLSERVER_HABILITADO = False."
        ) from e


def sqlserver_tabela_existe(tabela: str) -> bool:
    """INFORMATION_SCHEMA em vez de capturar erro de 'objeto inválido': independe do idioma
    da mensagem do servidor."""
    consulta = (
        f"SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{SQLSERVER_SCHEMA}' AND TABLE_NAME = '{tabela}'"
    )
    return ler_sqlserver_query(consulta).first()["n"] > 0


def colunas_sqlserver(tabela: str) -> set[str]:
    """Nomes de coluna existentes na tabela, via INFORMATION_SCHEMA (leitura, não dispara DDL).
    Vazio tanto se a tabela não existir quanto se não tiver colunas — os dois casos tratam-se
    como "sem schema prévio" por quem chama."""
    if not sqlserver_tabela_existe(tabela):
        return set()
    consulta = (
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{SQLSERVER_SCHEMA}' AND TABLE_NAME = '{tabela}'"
    )
    return {linha["COLUMN_NAME"] for linha in ler_sqlserver_query(consulta).collect()}


def escrever_sqlserver(df: DataFrame, tabela: str, modo: str, truncar: bool = False) -> None:
    """format("sqlserver") nativo, NÃO "jdbc": Databricks Serverless recusa DML (INSERT/overwrite)
    via format("jdbc") genérico — UNSUPPORTED_DATA_SOURCE_WRITE, "only ... sqlserver ... allowed
    to run DML on serverless" (achado em execução real, 02/10). Leitura continua via jdbc
    (ler_sqlserver_query) porque SELECT não é DML e esse caminho não é bloqueado.
    truncar=True preserva a tabela (índices, permissões) em vez de dropar e recriar — MAS só quando
    o schema já bate. O Delta evolui sozinho (mergeSchema) quando uma coluna de negócio nova aparece
    (ex.: preco_custo, 08/10); o SQL Server não acompanha sozinho, e o conector recusa escrever uma
    coluna que a tabela não tem (COLUMN_NOT_DEFINED_IN_TABLE, achado em execução real, 09/10). Se o
    DataFrame tiver coluna nova, a escrita vira "não truncar": o próprio conector dropa e recria a
    tabela com o schema atual — sem nenhuma DDL manual (ver incidente do pymssql/SIGABRT, 08/10,
    que é exatamente o tipo de solução que isto evita repetir)."""
    colunas_existentes = colunas_sqlserver(tabela)
    if truncar and colunas_existentes and not set(df.columns) <= colunas_existentes:
        novas = sorted(set(df.columns) - colunas_existentes)
        print(f"  {tabela}: schema evoluiu (coluna(s) nova(s) {novas}) — recriando a tabela no SQL Server.")
        truncar = False

    escritor = (
        df.write.format("sqlserver").options(**_opcoes_escrita_sqlserver())
        .option("dbtable", f"{SQLSERVER_SCHEMA}.{tabela}")
        .mode(modo)
    )
    if truncar:
        escritor = escritor.option("truncate", "true")
    escritor.save()


def sincronizar_historico_sqlserver(tabela: str, caminho_delta: str, chave: str = "ciclo_id") -> int:
    """Delta é a fonte da verdade; o SQL Server recebe apenas as chaves (`ciclo_id` nas tabelas
    Gold, `log_id` nos logs) que ainda não tem. Idempotente e autorreparável: uma falha de rede
    é compensada na execução seguinte, sem duplicar nem perder linhas. Retorna quantas chaves
    foram enviadas.

    Append (INSERT) não adiciona coluna sozinho: se o Delta ganhou coluna nova (mergeSchema) e o
    SQL Server ainda não tem, a tentativa de append falharia com COLUMN_NOT_DEFINED_IN_TABLE (achado
    em execução real, 09/10). Nesse caso, em vez de enviar só as chaves faltantes, resincroniza o
    histórico inteiro de uma vez (overwrite sem truncar = o conector dropa e recria com o schema
    atual); nas execuções seguintes, sem mais drift, volta ao envio incremental normal.

    ponytail: compara conjuntos no driver e filtra com isin(); com dezenas de milhares de chaves
    (logs após anos), trocar por left_anti join entre os dois DataFrames."""
    df_delta = ler_delta(caminho_delta)
    colunas_existentes = colunas_sqlserver(tabela)
    if colunas_existentes and not set(df_delta.columns) <= colunas_existentes:
        novas = sorted(set(df_delta.columns) - colunas_existentes)
        total = df_delta.count()
        print(f"  {tabela}: schema evoluiu (coluna(s) nova(s) {novas}) — resync completo do histórico ({total} linha(s)).")
        escrever_sqlserver(df_delta, tabela, "overwrite", truncar=False)
        return total

    chaves_delta = {linha[0] for linha in df_delta.select(chave).distinct().collect()}
    chaves_sql: set = set()
    if sqlserver_tabela_existe(tabela):
        consulta = f"SELECT DISTINCT {chave} FROM {SQLSERVER_SCHEMA}.{tabela}"
        chaves_sql = {linha[0] for linha in ler_sqlserver_query(consulta).collect()}

    faltantes = sorted(chaves_delta - chaves_sql)
    if faltantes:
        pendentes = df_delta.where(F.col(chave).isin(faltantes))
        escrever_sqlserver(pendentes, tabela, "append")
    return len(faltantes)


print(f"Config OK | scope={SECRET_SCOPE} | lakehouse={CONTAINER_LAKEHOUSE}/{RAIZ_LAKEHOUSE} | origem_raw={PASTA_LANDING}")