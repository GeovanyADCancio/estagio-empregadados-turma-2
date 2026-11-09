# 🚀 Real-Time Ecommerce Analytics Pipeline

**Squad 2 · Dupla 3**

👥 **Eduardo Souza e Aliedson Silva**

*Programa de Estágio em Engenharia de Dados*

`Azure Data Lake Gen2` · `Databricks Serverless` · `PySpark` · `Delta Lake` · `Azure SQL Server` · `pandas` · `Machine Learning`

---

## 📑 Sumário

1. Visão geral
2. Contexto
3. Arquitetura
4. Tecnologias
5. Estrutura do Data Lake
6. Camadas Bronze, Silver e Gold
7. Notebooks
8. Qualidade dos dados
9. Segurança
10. Fluxo de execução
11. Machine Learning · EDA e engenharia de features (P1)
12. Resultado final

---

## 1. 🔭 Visão geral

Este projeto implementa um pipeline completo de Engenharia de Dados para o processamento de pedidos de um e-commerce em tempo real.

A solução utiliza uma arquitetura **Lakehouse**, que combina armazenamento, processamento distribuído e camadas analíticas para transformar dados brutos em informações confiáveis para o negócio.

**O pipeline realiza:**

- 📥 ingestão incremental de dados;
- 🗂️ armazenamento em camadas Bronze, Silver e Gold;
- ✅ validação de qualidade dos dados;
- 🧹 tratamento de inconsistências;
- 📊 geração de indicadores de negócio;
- 📡 monitoramento operacional;
- 🗄️ disponibilização dos dados no Azure SQL Server;
- 🤖 preparação dos dados para Machine Learning (EDA e engenharia de features do P1 · previsão de vendas da tarde).

---

## 2. 🎯 Contexto

O objetivo da solução é criar uma estrutura capaz de acompanhar o comportamento dos pedidos de um e-commerce, permitindo maior visibilidade operacional e suporte à tomada de decisão.

| Necessidade | Como o pipeline atende |
|---|---|
| Acompanhar novos pedidos continuamente | Ingestão incremental em ciclos de *polling* |
| Garantir a confiabilidade dos dados | Regras de qualidade e quarentena na Silver |
| Identificar problemas de qualidade | Motivo registrado em cada pedido reprovado |
| Disponibilizar métricas atualizadas | KPIs recalculados a cada ciclo na Gold |
| Gerar alertas de possíveis anomalias | Mural de alertas publicado na Gold |

---

## 3. 🏗️ Arquitetura

```text
                 RAW · micro-lotes
                         │
   ┌─────────── Azure Data Lake Gen2 · squad2 ───────────┐
   │                     ▼                               │
   │          ┌──────────────────────┐                   │
   │          │  BRONZE              │                   │
   │          │  dados brutos        │                   │
   │          │  auditoria           │                   │
   │          └──────────┬───────────┘                   │
   │             ┌───────┴────────┐                      │
   │             ▼                ▼                      │
   │   ┌──────────────────┐ ┌──────────────────┐         │
   │   │  SILVER          │ │  QUARENTENA      │         │
   │   │  tratamento      │ │  dados           │         │
   │   │  data quality    │ │  reprovados      │         │
   │   └────────┬─────────┘ └────────┬─────────┘         │
   │            │                    │ alertas           │
   │            └─────────┬──────────┘                   │
   │                      ▼                              │
   │          ┌──────────────────────┐                   │
   │          │  GOLD                │                   │
   │          │  KPIs                │                   │
   │          │  alertas             │                   │
   │          └──────────┬───────────┘                   │
   └─────────────────────┼───────────────────────────────┘
                         ▼
                 AZURE SQL SERVER
```

---

## 4. 🧰 Tecnologias

| Tecnologia | Utilização |
|---|---|
| **Azure Data Lake Storage Gen2** | Armazenamento dos dados |
| **Databricks Serverless** | Ambiente de processamento |
| **Apache Spark / PySpark** | Processamento distribuído |
| **Delta Lake** | Armazenamento das camadas analíticas |
| **Azure SQL Server** | Disponibilização dos indicadores |
| **Databricks Secret Scope** | Gerenciamento seguro das credenciais |
| **GitHub** | Versionamento do projeto |

---

## 5. 📂 Estrutura do Data Lake

```text
squad2/
├── bronze/
│   └── ecommerce_pedidos/
├── silver/
│   └── ecommerce_pedidos/
├── quarantine/
│   └── ecommerce_pedidos/
├── gold/
│   └── ecommerce_pedidos/
│       ├── <kpis e alertas>
│       ├── ml_features_itens_pedido    ← features por pedido (ML)
│       ├── ml_features_itens_hora      ← features por hora (ML)
│       └── ml_features_p1_dia          ← features e alvo do P1 por dia (ML)
└── metadata/
    └── ecommerce_pedidos/
```

---

## 6. 🧱 Camadas Bronze, Silver e Gold

| | 🥉 **Bronze** | 🥈 **Silver** | 🥇 **Gold** |
|---|---|---|---|
| **Objetivo** | Armazenar os dados recebidos da origem, mantendo o histórico original | Transformar dados brutos em dados confiáveis para consumo analítico | Gerar indicadores e informações estratégicas para o negócio |
| **Responsabilidades** | Identificar novos arquivos · ingestão incremental · informações de auditoria · gravação em Delta · controle de arquivos processados | Regras de qualidade · padronização de campos · tratamento de inconsistências · validação de tipos · quarentena de registros inválidos | Cálculo de KPIs · geração de alertas · consolidação de métricas · publicação no Azure SQL Server |
| **Características** | Não aplica transformação de negócio · preserva o dado original · garante rastreabilidade | Dados preparados para análise · maior confiabilidade · controle de qualidade aplicado | Camada de consumo do negócio |

### 📊 Indicadores gerados na Gold

| Indicador | Objetivo |
|---|---|
| 📦 Volume de pedidos (5 min, minuto a minuto; com o pipeline em dia, segue o relógio) | Monitoramento operacional |
| 💳 Ticket médio | Análise financeira |
| ❌ Taxa de cancelamento (qualquer janela de 30 min, deslizando a cada 5 min) | Identificação de problemas |
| 💰 Receita diária | Acompanhamento comercial |
| 🚨 Alertas | Detecção de anomalias |

---

## 7. 📓 Notebooks

| Notebook | Objetivo | Responsabilidades | Característica |
|---|---|---|---|
| ⚙️ `00_setup_config` | Preparar o ambiente do projeto | Configurar credenciais, caminhos do Data Lake, conexões e funções compartilhadas | Notebook base, utilizado pelos demais |
| 🗄️ `01_carga_sqlserver` | Realizar a carga inicial dos dados | Validar a conexão, carregar os dados e confirmar a gravação no banco | Cria o espelho inicial no SQL Server |
| 🔍 `02_eda_ecommerce` | Analisar os dados de origem | Avaliar schema, volume, qualidade e nulos, e gerar o contrato de dados | Primeira validação analítica antes do pipeline |
| 🥉 `03_ingestao_bronze` | Ingestão na camada Bronze | Ler micro-lotes, adicionar metadados e salvar em Delta Lake | Mantém os dados originais, sem transformação |
| 🥈 `04_ingestao_silver` | Tratamento dos dados | Aplicar regras de qualidade, padronizar e enviar inconsistências para a quarentena | Cria uma camada confiável para análise |
| 🥇 `05_ingestao_gold` | Gerar indicadores de negócio | Calcular KPIs, gerar alertas e publicar resultados | Responsável pela camada analítica |
| 🔁 `06_orquestrador` | Controlar a execução do pipeline | Executar Bronze → Silver → Gold e controlar ciclos, logs e erros | Automatiza o processamento completo |
| 🔎 `07_explorador_squad2` | Explorar e validar os dados | Consultar estruturas, tabelas e resultados das camadas | Somente leitura |
| ✔️ `08_verificacao_sqlserver` | Validar a integração com o SQL Server | Conferir tabelas, registros e dados publicados | Garante a comunicação entre Databricks e banco |
| ⏱️ `09_teste_tempo_real` | Validar o fluxo de ponta a ponta | Testar o processamento, a chegada de arquivos e a latência | Simula o cenário real de operação |
| 📈 `10_monitoramento_pipeline` | Monitorar a saúde do pipeline | Acompanhar erros, ciclos, latência e qualidade dos dados | Camada de observabilidade operacional |
| ✅ `11_validacao_camadas` | Validar a entrega camada por camada | Comparar Bronze × raw, Silver × Bronze e Gold × Silver pedido a pedido, recalcular os KPIs, conferir o SQL Server e mostrar a Gold em gráficos | Somente leitura; veredito por requisito do plano e da planilha |
| 🔍 `12_ml_eda_itens` | **ML · Tarefa 1 · Análise exploratória** | Fonte e cobertura do gerador, colunas, qualidade, ligações, fórmula do `valor_total`, distribuições, categorias, padrões manhã × tarde, adequação ao P1 e insights | Não grava dados; relatório em `docs/12_ml_eda_itens/` |
| 🧮 `13_ml_features_itens` | **ML · Tarefa 2 · Engenharia de features** | Cria e grava 3 tabelas de features na Gold, codifica as categóricas (0/1) e normaliza as numéricas nas tabelas `ml_modelo_*`, analisa os padrões, valida contra as tarefas e a planilha e gera o relatório completo | Rodar depois do `12`; relatório em `docs/13_ml_features_itens/` |

---

## 8. ✅ Qualidade dos dados

O pipeline possui validações para garantir a confiabilidade dos dados:

| Validação | Camada |
|---|---|
| 📋 Existência das colunas obrigatórias | 🥉 Bronze |
| 🔢 Validação de tipos | 🥉 Bronze · 🥈 Silver |
| 💲 Valores financeiros válidos | 🥈 Silver |
| 📅 Datas consistentes | 🥈 Silver |
| 🏷️ Status permitidos | 🥈 Silver |
| 🔁 Controle de duplicidades | 🥉 Bronze · 🥈 Silver |
| 📐 Regras de negócio | 🥇 Gold |

> Registros que não passam nas validações vão para a **quarentena** com o motivo da reprovação; nenhum dado é descartado sem registro.

> 🏷️ **Status permitidos (regra 5):** `PROCESSANDO`, `PAGAMENTO APROVADO`, `CANCELADO`, `PENDENTE`, `AGUARDANDO`, `EM SEPARAÇÃO`, `ENVIADO` e `ENTREGUE`.
> `ERROR` é dado defeituoso e vai para a quarentena. A lista fica no `02_eda_ecommerce` (`STATUS_ESPERADOS`) e chega à Silver pelo contrato da EDA.

---

## 9. 🔐 Segurança

As credenciais da solução **não ficam armazenadas no código**. A autenticação utiliza o **Databricks Secret Scope**.

| Informação protegida |
|---|
| 🔑 Azure Client ID |
| 🔑 Tenant ID |
| 🔑 Client Secret |
| 🔑 Credenciais do SQL Server |

> **Por que Secret Scope, e não `.env`:** as orientações do estágio pedem variáveis de ambiente (`.env`). No Databricks Serverless,
> um `.env` precisaria existir como arquivo no workspace, ao lado do código. O Secret Scope `mercadata-ecommerce-pedidos` guarda as mesmas
> 7 chaves (`ADLS_CLIENT_ID`, `ADLS_TENANT_ID`, `ADLS_CLIENT_SECRET`, `SQL_HOST`, `SQL_DATABASE`, `SQL_USERNAME`, `SQL_PASSWORD`) fora de
> qualquer arquivo, com acesso controlado pelo Databricks, e o valor aparece como `[REDACTED]` se for impresso.
> Mesmo sem `.env` no projeto, o `.gitignore` mantém `.env`, `.env.*` e `*.env`, como pedem as orientações.

---

## 10. ▶️ Fluxo de execução

### 🆕 Primeira execução

| Passo | Notebook | O que faz |
|:---:|---|---|
| 1 | ⚙️ `00_setup_config` | Prepara e valida o ambiente |
| 2 | 🔍 `02_eda_ecommerce` | Analisa a origem e publica o contrato de dados |
| 3 | 🗄️ `01_carga_sqlserver` | Faz a carga inicial no SQL Server |
| 4 | 🥉 `03_ingestao_bronze` | Ingere os micro-lotes na Bronze |
| 5 | 🥈 `04_ingestao_silver` | Trata os dados e separa a quarentena |
| 6 | 🥇 `05_ingestao_gold` | Gera os indicadores e os alertas |
| 7 | 🔁 `06_orquestrador` | Passa a executar o pipeline em ciclos |

### 🔄 Execução contínua

```text
   ┌──────────────────┐
   │  06_orquestrador │◀─────────────────────────┐
   └────────┬─────────┘                          │
            ▼                                    │
         BRONZE ──▶ SILVER ──▶ GOLD ─────────────┘
                                   próximo ciclo
```

> ⏰ **Agendamento (Job a cada 15 min).** O `06_orquestrador` roda num Job do Databricks (Workflows → Jobs) a cada 15 minutos, com no máximo
> uma execução por vez: é o ritmo de atualização do dashboard. Cada execução roda Bronze, Silver e Gold no primeiro ciclo e continua enquanto
> chegam arquivos. A regra 6 depende disso: com o pipeline em dia e sem pedido novo, o placar dela anda com o relógio, e uma queda do
> e-commerce no pico (cerca de 15 minutos sem nenhum pedido) vira alerta uns 22 minutos depois do último pedido, desde que a Gold rode durante a queda.

> 🔒 **Uma execução por vez.**
> - O `06_orquestrador` cria uma trava em `metadata/`. Enquanto ela está ativa, `03`, `04` e `05` rodados à mão se recusam a processar.
> - A gravação da Bronze leva junto a versão da tabela que ela leu, e o Delta aceita só uma gravação a partir de cada versão. Assim, duas execuções simultâneas não duplicam micro-lotes.
> - Se mesmo assim um micro-lote aparecer duplicado, a Silver usa só a 1ª ingestão dele. O procedimento de reparo está no fim do `03_ingestao_bronze`.

### 🧪 Validação

| Notebook | Quando usar |
|---|---|
| 🔎 `07_explorador_squad2` | Para conferir os dados gravados nas camadas |
| ✔️ `08_verificacao_sqlserver` | Para conferir as tabelas publicadas no banco |
| ⏱️ `09_teste_tempo_real` | Antes e depois de uma janela de tempo real |
| 📈 `10_monitoramento_pipeline` | A qualquer momento, para ver a saúde do pipeline |
| ✅ `11_validacao_camadas` | Antes de abrir o PR e depois de cada janela de tempo real, com o orquestrador parado |

### 🔧 Quando a lista de status permitidos mudar

| Passo | O que fazer |
|:---:|---|
| 1 | Pausar o Job e conferir que o `06_orquestrador` não está rodando |
| 2 | Ajustar `STATUS_ESPERADOS` no `02_eda_ecommerce` e rodá-lo (publica o contrato novo) |
| 3 | Rodar o `05_ingestao_gold` e o `11_validacao_camadas`; reativar o Job |

> O que o `04_ingestao_silver` já mandou para a quarentena não volta sozinho: a Silver só lê micro-lotes novos da Bronze. Vale para os micro-lotes que chegarem depois da mudança.

---

## 11. 🤖 Machine Learning · EDA e engenharia de features (P1)

Fase de ML em streaming definida na planilha `machine_learning_squad_2`. Para a dupla:

| Item | Definição |
|---|---|
| **Problema** | P1 · Previsão de vendas do período da tarde: receita e nº de pedidos entre 12h e 18h, a partir do histórico e do movimento da manhã |
| **Base da etapa 1** | itens (`ecommerce_itens_pedido`), ligada a pedidos, produtos e categorias |
| **Modelo** | LightGBM com lags · Modelo B (machine learning), comparado ao SARIMAX da dupla Leandro e Kauan na mesma janela de teste |
| **Features pedidas** | lags de 1 a 24h, média móvel, hora, dia da semana e mix de categorias |
| **Métricas** | MAE, WAPE e RMSE contra o baseline "mesma hora do dia anterior" |

### Tarefas da semana

| Tarefa | Onde |
|---|---|
| **1 · Análise exploratória (EDA)** — entender se os dados estão adequados, conhecer a tabela e gerar insights para o modelo | `12_ml_eda_itens` |
| **2 · Engenharia de features** — preparar os dados para o modelo e criar colunas numéricas que ajudem a identificar padrões; transformar categóricas em numéricas e normalizar (orientação do orientador) | `13_ml_features_itens` |
| **Validação** — cada exigência das tarefas e da planilha, com status | `13_ml_features_itens` · seção 2.8 |

### Tabelas de features (Gold · `squad2/gold/ecommerce_pedidos/`)

| Tabela | Uma linha por | Principais colunas |
|---|---|---|
| `ml_features_itens_pedido` | pedido | nº de itens, SKUs, marcas e categorias, valor bruto e líquido, valor médio do item, desconto, preços, % vendido por peso, categoria e marca dominantes, mix, itens inválidos, cesta de SKUs |
| `ml_features_itens_hora` | data + hora (Brasília) | receita, pedidos, ticket, mix de categorias, lags de 1 a 24h, média móvel, hora, dia da semana |
| `ml_features_p1_dia` | dia | o que se sabe da manhã até 12h (features) e a receita e os pedidos da tarde (alvo) |

| `ml_modelo_itens_pedido` · `ml_modelo_itens_hora` · `ml_modelo_p1_dia` | as mesmas | **entrada do modelo**: categóricas em 0/1 (one-hot), numéricas padronizadas (média 0, desvio 1), alvos na escala real; média e desvio de cada coluna em `metadata/ecommerce_pedidos/ml_normalizacao_itens.json` |

**Regras:** `valor_total = Σ quantidade × (preço − desconto por unidade)` (o frete fica fora) · receita só de pedidos aprovados na Silver ·
itens inválidos são marcados, não apagados · hora sem micro-lote fica vazia (dado desconhecido), não zero.

### Relatórios de cada execução

Os relatórios ficam só na pasta `docs/` (não aparecem nos notebooks). Cada etapa traz o que foi feito, o que obtivemos, a interpretação e o impacto no modelo:

```text
docs/
├── 12_ml_eda_itens/
│   ├── relatorio_MM-DD-AAAA_HH-MM-SS.md     EDA (texto e tabelas)
│   ├── relatorio_MM-DD-AAAA_HH-MM-SS.html   EDA com os gráficos
│   ├── indice.md                            todas as execuções
│   └── registro_eda.json                    resultados da EDA usados pelo 13
└── 13_ml_features_itens/
    ├── relatorio_MM-DD-AAAA_HH-MM-SS.md     relatório completo: EDA + features + validação + roteiro da apresentação
    ├── relatorio_MM-DD-AAAA_HH-MM-SS.html   o mesmo conteúdo com os gráficos
    └── indice.md                            todas as execuções
```

### Como rodar

| Passo | Notebook | O que faz |
|:---:|---|---|
| 1 | 🔍 `12_ml_eda_itens` | Tarefa 1 · EDA da base de itens; grava o relatório da EDA e o `registro_eda.json` |
| 2 | 🧮 `13_ml_features_itens` | Tarefa 2 · recalcula as features a partir de todo o raw, grava as 3 tabelas (overwrite), valida e gera o relatório completo |

> ⚠️ **Situação atual:** as features e as tabelas para o modelo estão prontas. O modelo (LightGBM) será implementado na próxima semana e a comparação
> com a outra dupla do P1 vem mais à frente. Para treinar, ainda faltam dias completos (tarde inteira e parte da manhã com micro-lotes). Com dados novos, basta rodar o `12` e o `13` de novo, nessa ordem.

---

## 12. 🏁 Resultado final

O projeto entrega uma solução completa de Engenharia de Dados capaz de:

- 📥 receber dados continuamente;
- 🧱 processar informações em camadas;
- ✅ garantir qualidade;
- 🧭 manter rastreabilidade;
- 📊 gerar indicadores;
- 🗄️ disponibilizar dados para análise;
- 🤖 preparar os dados para Machine Learning.

```text
   Azure Data Lake Gen2
            │
            ▼
   Databricks + PySpark
            │
            ▼
   Delta Lake
            │
            ▼
   KPIs de negócio
            │
            ▼
   Azure SQL Server
```
