# 🚀 Real-Time Ecommerce Analytics Pipeline

**Squad 2 · Dupla 3**

👥 **Eduardo Souza e Aliedson Silva**

*Programa de Estágio em Engenharia de Dados*

`Azure Data Lake Gen2` · `Databricks Serverless` · `PySpark` · `Delta Lake` · `Azure SQL Server`

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
11. Resultado final

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
- 🗄️ disponibilização dos dados no Azure SQL Server.

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
| 📦 Volume de pedidos | Monitoramento operacional |
| 💳 Ticket médio | Análise financeira |
| ❌ Taxa de cancelamento | Identificação de problemas |
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

---

## 9. 🔐 Segurança

As credenciais da solução **não ficam armazenadas no código**. A autenticação utiliza o **Databricks Secret Scope**.

| Informação protegida |
|---|
| 🔑 Azure Client ID |
| 🔑 Tenant ID |
| 🔑 Client Secret |
| 🔑 Credenciais do SQL Server |

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

---

## 11. 🏁 Resultado final

O projeto entrega uma solução completa de Engenharia de Dados capaz de:

- 📥 receber dados continuamente;
- 🧱 processar informações em camadas;
- ✅ garantir qualidade;
- 🧭 manter rastreabilidade;
- 📊 gerar indicadores;
- 🗄️ disponibilizar dados para análise.

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
