# Documentação Looker Studio — Squad 2 / Dupla 2 (Kauan e Leandro)

Guia definitivo das fontes de dados, gráficos, cores e automação do dashboard de insights de `ecommerce_clientes`. Última atualização: 22/10/2026.

---

## 1. Conexão Databricks ↔ Looker Studio

1. No Databricks: **SQL Warehouses** → copie o **Server hostname** e o **HTTP path** do warehouse (Serverless Small é suficiente)
2. Gere um **Personal Access Token**: Settings → Developer → Access tokens
3. No Looker Studio: **Recursos → Fontes de dados gerenciadas → Criar** → conector **Databricks** → cole host, HTTP path e token → Autenticar
4. Selecione o schema indicado em cada fonte abaixo

> ⚠️ Sete a **Data freshness** da fonte para o menor intervalo disponível e use o botão **Atualizar dados** no relatório para forçar valores atuais.

---

## 2. Fontes de dados (em ordem de prioridade)

| # | Fonte | Schema | Atualização | Uso |
|---|---|---|---|---|
| 1 | `gold_insights_clientes` | `sqlserver_catalog.squad2` | **Tempo real** (job a cada 15 min) | Cards KPI com severidade |
| 2 | `gold_insights_clientes_historico` | `sqlserver_catalog.squad2` | **Tempo real** (append por ciclo) | Gráfico de evolução temporal |
| 3 | `vw_provedor_lider_detalhe` | `workspace.default` | A cada execução do 07 | Gráfico de provedores |
| 4 | `vw_clientes_por_ano` | `workspace.default` | A cada execução do 07 | Cadastrados vs compraram por ano |
| 5 | `vw_compradores_por_ano` | `workspace.default` | A cada execução do 07 | Compradores (pedidos) por ano |
| 6 | `vw_insights_clientes_kpi` | `workspace.default` | A cada execução do 07 | Cards KPI alternativos |
| 7 | `vw_fontes_compradores` | `workspace.default` | Auditoria | Tabela de proveniência |

**Padronização (22/10):** Kauan e Leandro usam a MESMA fonte principal — `sqlserver_catalog.squad2.gold_insights_clientes` — atualizada automaticamente pelo job. Não conectar o Looker em views antigas.

---

## 3. Gráficos (configuração)

### 3.1 Cards KPI (fonte 1)
- Tipo: **Scorecard**
- Métrica: `valor`, filtro: `insight = '<nome do insight>'`
- Cor do card: paleta abaixo conforme `severidade`

### 3.2 Provedor com mais cadastros (fonte 3)
- Tipo: **Combo** (barra + linha)
- Dimensão: `provedor` | Métrica 1 (barra): `quantidade` | Métrica 2 (linha): `percentual`
- Ordenar por `quantidade` decrescente; 9 provedores no total

### 3.3 Cadastros vs Compraram por ano (fonte 4)
- Tipo: **Colunas** | Dimensão: `ano` | Métricas: `cadastrados` e `compraram`
- Base: Silver de clientes (363-366 cadastros, 4 com compra nos pedidos)

### 3.4 Compradores (pedidos) por ano (fonte 5)
- Tipo: **Colunas** | Dimensão: `ano` | Métrica: `compraram`
- **Rotular como "Compradores (pedidos)"**
- ⚠️ 9.998 dos 10.002 compradores NÃO têm cadastro (ids de pedidos em escala diferente — ver `vw_fontes_compradores`)

### 3.5 Evolução dos insights (fonte 2)
- Tipo: **Linha** | Dimensão: `calculado_em` | Métrica: `valor`
- Filtro: `insight = '<nome>'` (um gráfico por insight)
- No SQL Server o histórico NÃO tem `cor_status` (7 colunas) — derive a cor de `severidade`

---

## 4. Paleta de cores (padrão visual)

| Severidade/uso | Cor | Hex |
|---|---|---|
| Barras principais | Azul primário | `#1F4E8C` |
| OK | Verde | `#2E7D32` |
| ALERTA | Amarelo | `#F2B705` |
| CRÍTICO | Vermelho | `#C0392B` |
| SEM_DADOS | Cinza | `#8C8C8C` |
| Fundo/detalhe | Azul claro | `#D5E8F0` |

---

## 5. Automação (Jobs)

| Job | ID | Cadência | O que faz |
|---|---|---|---|
| `Gold KPIs - Atualizacao 15min` | 404583810112425 | 15 min | Notebook 06: recalcula KPIs na Gold |
| `insights_clientes_atualizacao_15min` | 900697359026405 | 15 min | Notebook 07: insights → Delta + SQL Server + views UC |

Esteira: `06_ingestao_gold` → `07_insights_clientes` → SQL Server (`squad2.gold_insights_clientes` + histórico) → Looker Studio.

⚠️ Não rodar manualmente cópias do 07 (ex.: notebook 09) — gravam nas mesmas tabelas e podem conflitar com o job.

---

## 6. Semântica das janelas (fonte 1 — tempo real)

| Insight | Janela |
|---|---|
| Novos clientes | Últimos 10 minutos |
| Provedores desconhecidos | Últimos 14.400 minutos (10 dias) |
| E-mails duplicados / atualização anterior | Últimos 60 minutos |
| Provedor líder / tempo 1ª compra | Base acumulada da janela do KPI |

> As views UC (fontes 3-6) são acumuladas — não comparar diretamente com os valores de janela da fonte 1.