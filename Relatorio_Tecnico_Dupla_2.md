# Relatório Técnico Completo — Dupla 2 (Kauan & Leandro)

**Squad 2 — Real Time for Business | Dupla 2**  
**Data:** 05 de Outubro de 2026  
**Branch:** `feat/squad2/kauan-leandro`  
**Tech Lead:** Geovany Aparecido Duarte Câncio  
**Scrum Master:** Patrick Marangoni Neri da Silva  
**Product Owner:** Vinícius de Souza Silveira  

> **Para converter em PDF:** Abra este arquivo no navegador (ou no VS Code) e use **Print > Save as PDF**.

---

## 1. Visão Geral do Dia

A sessão de hoje teve como objetivo principal **alinhar os notebooks da Dupla 2 com o padrão técnico definido pelo Tech Lead (Geovany)**, após recebermos o código de referência da Dupla 1 (Lucas & Zaiden).

**Principais mudanças realizadas:**

- MERGE INTO em todas as missões Silver (idempotência)
- optimizeWrite + autoCompact nas escritas Delta
- Novas validações técnicas (SKU 5-60 chars, is_ativo, id_categoria, nome_categoria)
- Cabeçalhos padronizados em todos os 8 notebooks (4 principais + 4 documentações)
- Criação do README_DUPILA_2.md com documentação técnica completa
- Commit e push para a branch `feat/squad2/kauan-leandro`
- Documentações revisadas e atualizadas com MERGE INTO e novas validações

---

## 2. MERGE INTO — Por que e como foi feito

### O problema do append

Antes do alinhamento, todas as missões Silver persistiam dados com `mode('append')`. Isso significa que se o notebook fosse executado duas vezes, os registros seriam **duplicados** na tabela Silver.

**Exemplo prático:** Se 42 produtos fossem inseridos e o notebook rodasse de novo, haveria 84 registros (42 duplicados).

### A solução: MERGE INTO (Upsert)

O MERGE INTO resolve esse problema porque ele faz um **upsert**:
- Se o registro **já existe** (match pela chave de negócio) → **ATUALIZA**
- Se o registro **não existe** → **INSERE**

Isso garante **idempotência absoluta** — reexecutar o notebook quantas vezes quiser nunca duplica dados. Esta decisão foi alinhada com o Tech Lead como padrão oficial para toda a Squad 2.

### Implementação técnica

Foi criada uma função genérica `merge_into(df, tabela_uc, chave)` na célula de configuração do notebook 08. Esta função:

1. Verifica se a tabela já existe no Unity Catalog
2. Se **não existe**: cria com append (primeira carga)
3. Se **existe**: aplica `DeltaTable.forName(spark, tabela).merge()` com `whenMatchedUpdateAll()` e `whenNotMatchedInsertAll()`
4. Também foi criada `append_quarentena()` para manter log de rejeições (append, sem MERGE — mantém histórico de falhas)

### Chaves de negócio por missão

| Missão | Tabela Silver | Chave MERGE |
| --- | --- | --- |
| 02 | silver_ecommerce_itens_pedido | id_item_pedido |
| 03 | silver_ecommerce_produtos | sku |
| 04 | silver_ecommerce_categorias | id_categoria |
| 05 | silver_ecommerce_rastreamento | id_rastreamento |
| 07 | silver_ecommerce_enderecos | id_endereco |

---

## 3. optimizeWrite + autoCompact — Por que e como

### O problema

Sem otimização, o Delta Lake cria muitos arquivos pequenos a cada escrita (**small file problem**). Isso degrada a performance de leitura porque o Spark precisa abrir centenas de arquivos pequenos em vez de poucos arquivos grandes.

### A solução

Foram configuradas duas propriedades Spark globais no início dos notebooks 08 e 09:

```python
spark.conf.set("spark.databricks.delta.properties.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.autoOptimize.autoCompact", "true")
```

- **optimizeWrite:** agrupa arquivos pequenos em arquivos maiores durante a escrita
- **autoCompact:** compacta arquivos automaticamente após operações de escrita

Ambas as propriedades também foram passadas como `.option()` nas funções `merge_into()` e `append_quarentena()` para garantir que todas as escritas sejam otimizadas.

---

## 4. Novas Validações Técnicas — Por que e quais

### Contexto

A Dupla 1 implementou validações técnicas mais ricas que as nossas. Alinhamos nossas validações para seguir o mesmo padrão, adicionando verificações de SKU e is_ativo (produtos) e id_categoria e nome_categoria (categorias).

### Missão 03 (ecommerce_produtos) — validações adicionadas

| Motivo | Condição | Ação |
| --- | --- | --- |
| SKU_NULO | sku é NULL | Quarentena |
| SKU_TAMANHO_INVALIDO | len(sku) < 5 ou > 60 | Quarentena |
| IS_ATIVO_NULO | is_ativo é NULL | Quarentena |
| PRECO_NULO | preco_lista é NULL | Quarentena |
| PRECO_MENOR_OU_IGUAL_ZERO | preco_lista ≤ 0 | Quarentena |
| PRECO_MAIOR_OU_IGUAL_5000 | preco_lista ≥ 5000 | Quarentena |

### Missão 04 (ecommerce_categorias) — validações adicionadas

| Motivo | Condição | Ação |
| --- | --- | --- |
| ID_CATEGORIA_NULO | id_categoria é NULL | Quarentena |
| NOME_CATEGORIA_NULO | nome_categoria é NULL ou vazio | Quarentena |

**Impacto:** Antes dessas mudanças, a Missão 03 só validava `preco_lista` e a Missão 04 não tinha quarentena (apenas alerta de categorias raiz). Agora ambas têm validações técnicas completas com quarentena e tabela de rejeições própria.

---

## 5. Cabeçalhos Padronizados — Por que e como

### Contexto

O notebook da Dupla 1 começa com um cabeçalho rico contendo Squad, Integrantes, Tabelas de Escopo, Branch e Objetivo da Task. Nossos notebooks não tinham esse padrão. Alinhamos todos os 8 notebooks.

### Estrutura aplicada

```
# NN - Nome do Notebook
Squad 2 — Real Time for Business | Dupla 2
Integrantes: Kauan & Leandro
Tabelas de Escopo: (específicas de cada notebook)
Branch: feat/squad2/kauan-leandro
Objetivo da Task com lista numerada baseada nas missões reais
```

### Notebooks atualizados

| Notebook | Tipo | Status |
| --- | --- | --- |
| 04_ingestao_bronze | Principal | Cabeçalho criado |
| 06_ingestao_pedidos_bronze | Principal | Cabeçalho atualizado |
| 08_ingestao_silver_all | Principal | Cabeçalho atualizado |
| 09_ingestao_gold_kpis | Principal | Cabeçalho atualizado |
| 04_bronze_documentacao | Documentação | Cabeçalho padronizado |
| 05_silver_documentacao | Documentação | Cabeçalho padronizado |
| 08_11_documentacao_silver | Documentação | Cabeçalho padronizado |
| 09_documentacao_gold_kpis | Documentação | Cabeçalho padronizado |

### Correção do termo "micro-lote"

Inicialmente copiamos o termo "micro-lote" da Dupla 1, mas percebemos que nossa abordagem é diferente:
- **Dupla 1:** watermark temporal (lê apenas o delta da Bronze via `bronze_ingested_at > ultimo_processado`)
- **Nós:** anti-join por chave de negócio (lê a origem completa e filtra os já processados)

Corrigimos o cabeçalho para dizer "Leitura incremental via anti-join por chave de negócio (Lakehouse Federation)" em vez de "micro-lote".

---

## 6. Documentações Revisadas — O que mudou

### 08_11_documentacao_silver (principal alteração)

- Visão geral: 4 missões → **5 missões** (adicionada Missão 02)
- Estrutura: 12 células → **16 células** (reflete o notebook atual)
- Arquitetura: "append" → **"MERGE INTO por chave de negócio + optimizeWrite"**
- Validações Missão 03: adicionadas `SKU_NULO`, `SKU_TAMANHO_INVALIDO`, `IS_ATIVO_NULO`
- Validações Missão 04: adicionadas `ID_CATEGORIA_NULO`, `NOME_CATEGORIA_NULO`
- Tabelas UC: 8 → **12 tabelas** (adicionadas pedidos, itens, espera, quarentena_categorias)
- Resumo: todas as missões marcadas como **✅ Executado** com resultados reais
- Pontos pendentes: convertidos em **"pontos resolvidos"**

### Outras documentações

As documentações 04_bronze e 05_silver já estavam bem detalhadas (célula por célula). A documentação 09_gold_kpis também estava completa. Apenas os cabeçalhos foram padronizados.

---

## 7. README_DUPILA_2.md — Criação

### Contexto

A Dupla 1 criou um README extenso no GitHub com 5 seções. Criamos um README equivalente para a Dupla 2, seguindo a mesma estrutura mas com nossos dados reais.

### Seções do README

1. **Escopo e Governança:** Squad, Dupla, Branch, Tech Lead, 7 missões com volumetria + 3 KPIs Gold
2. **Arquitetura:** diagrama ASCII, arquitetura dual (ADLS + UC Federation), anti-join, MERGE INTO
3. **Ordem de Execução:** Sprint 1 + Sprint 2 com 6 passos detalhados
4. **Textos Trello:** 6 tasks prontas para copiar e colar no Trello
5. **Segurança:** .env, .gitignore, isolamento _dupla_2, flag EXECUTAR_JDBC
6. **Documentações:** tabela com notebooks principais + documentações

### Diferenciais vs Dupla 1

- 7 missões cobertas (eles cobrem 2 tabelas)
- Camada Gold documentada com 3 KPIs (eles não têm Gold)
- Arquitetura dual (ADLS + UC Federation) explicada
- Isolamento de squad com sufixo `_dupla_2` explicado

---

## 8. Arquitetura Dual — Por que não conseguimos usar só ADLS

### O problema

Nosso workspace está na **AWS** com compute **serverless**. O serverless na AWS bloqueia toda rede externa para serviços como Azure Storage. O DNS resolve domínios do Azure para um IP sinkhole (`192.168.200.20`), impedindo qualquer conexão direta.

### Por que a Dupla 1 "consegue" usar ADLS

A Dupla 1 injeta credenciais via `spark.read.options(**adls_options)`, que resolve o problema de **autenticação** (não pode setar configs globais no serverless), mas **NÃO resolve o problema de rede**. Provavelmente:
- O código deles também falha no serverless, ou
- Existe uma Unity Catalog external location configurada por um admin, ou
- Eles testaram em cluster clássico antes da mudança para serverless

### Nossa solução: Lakehouse Federation

Usamos `sqlserver_catalog.squad2.*` (Lakehouse Federation) que funciona no serverless porque o Unity Catalog gerencia a conexão — a rede sai pelo serviço do UC, não pelo compute serverless. É por isso que nossos notebooks 06, 08 e 09 funcionam.

### Resumo da arquitetura

| Camada | Origem | Funciona no serverless? |
| --- | --- | --- |
| Bronze (04) | ADLS Gen2 (Parquet) | Não — precisa cluster clássico |
| Bronze (06) | SQL Server (Federation) | Sim |
| Silver (05) | ADLS + UC fallback | Sim (fallback) |
| Silver (08) | SQL Server (Federation) | Sim |
| Gold (09) | Silver (Unity Catalog) | Sim |

---

## 9. Git — Commits e Branch

### Branch

`feat/squad2/kauan-leandro` (já existia e estava ativa)

### Commits realizados

1. **Commit 1:** Alinhamento Tech Lead (13 arquivos) — MERGE INTO, optimizeWrite, validações técnicas e notebook Gold KPIs
2. **Commit 2:** Padronização cabeçalhos (8 arquivos) — cabeçalhos e documentações
3. **Commit 3:** README (1 arquivo) — documentação técnica completa

### Pull Request

Não existe branch `dev` no repositório. O PR deve ser aberto para `main` via UI do Git provider (GitHub). A branch `feat/squad2/kauan-leandro` está pushed e pronta.

---

## 10. Resumo Final

### Status dos notebooks

| Notebook | Camada | Status |
| --- | --- | --- |
| 04_ingestao_bronze | Bronze | Cabeçalho atualizado |
| 05_ingestao_silver | Silver | Documentado (Kauan) |
| 06_ingestao_pedidos_bronze | Bronze | MERGE + cabeçalho |
| 08_ingestao_silver_all | Silver | MERGE INTO + validações |
| 09_ingestao_gold_kpis | Gold | 3 KPIs + optimizeWrite |

### Arquivos criados

- `README_DUPILA_2.md` — documentação técnica completa (6 seções)
- `Relatorio_Tecnico_Dupla_2.md` — este relatório

### Conclusão

Todos os notebooks da Dupla 2 estão alinhados com o padrão técnico da Dupla 1 e do Tech Lead. As principais melhorias foram:

1. **MERGE INTO** (idempotência) — reexecução nunca duplica dados
2. **optimizeWrite + autoCompact** (performance) — evita small file problem
3. **Validações técnicas mais ricas** — SKU, is_ativo, id_categoria, nome_categoria
4. **Cabeçalhos padronizados** — Squad, Dupla, Branch, Objetivo em todos os 8 notebooks
5. **Documentações revisadas** — 5 missões, MERGE INTO, 12 tabelas UC, pontos resolvidos
6. **README criado** — 6 seções, 7 missões, 3 KPIs, diagrama de arquitetura

A branch `feat/squad2/kauan-leandro` está pushed e pronta para PR.
