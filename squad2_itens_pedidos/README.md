# Squad 2 — `ecommerce_itens_pedido` em tempo real

**Dupla:** José Vieira & Tiago Ramos · **Frente:** Negócio em Tempo Real

Pipeline em PySpark no Databricks que lê os micro-lotes de itens de pedido gravados pelo gerador de tempo real
no Data Lake, organiza os dados em Bronze, Silver e Gold e publica os KPIs no Azure SQL Server.

```
raw/real-time-data/AAAA/MM/DD/HHMMSS/ecommerce_itens_pedido.parquet
        │  04 Bronze   (dado como veio + auditoria)
        ▼
workspace.squad2.bronze_ecommerce_itens_pedido        Delta, Unity Catalog
        │  05 Silver   (regras T1–T5 e N9, flags)
        │       ◄── squad2/silver/ecommerce_pedidos           (T2: pedido existe; traz dt_pedido)
        │       ◄── squad2/quarantine/ecommerce_pedidos       (T2: pedido reprovado)
        │       ◄── squad2/silver/ecommerce_produtos          (T4: preço x catálogo)
        ├──────────────► squad2/grupo2/quarantine/ecommerce_itens_pedido   reprovados + motivo
        ├──────────────► squad2/grupo2/waiting/ecommerce_itens_pedido      aguardando o pedido
        ▼
squad2/grupo2/silver/ecommerce_itens_pedido           Delta, append deduplicado por id_item_pedido
        │  06 Gold     (KPIs N6, N7, N8, N10)
        ▼
squad2/grupo2/gold/gold_*  (Delta)   +   squad2.gold_*  (Azure SQL Server)
```

## Notebooks

| Notebook | Etapa | Lê | Grava |
|---|---|---|---|
| `00_setup_config` | Parâmetros, `.env`, `adls_options`, caminhos, conexão SQL | `.env` | — |
| `01_extracao_itens_pedido` | Extração distribuída de todas as janelas | `raw` | DataFrame `df_itens` |
| `02_analise_itens_pedido` | Análise exploratória e qualidade | `01` | — |
| `03_carga_sql_server` | Carga direta ponta a ponta | `01` | `squad2.ecommerce_itens_pedido` |
| `04_bronze_ingestao_delta` | Bronze: micro-lotes novos, sem transformação | `raw` | `bronze_ecommerce_itens_pedido` |
| `05_silver_itens_pedido` | Silver: regras, flags, quarentena e espera | Bronze + Silver de pedidos e produtos | `silver/`, `quarantine/`, `waiting/` |
| `06_gold_itens_pedido` | Gold: KPIs, histórico, fato e qualidade | Silver | `gold/` e SQL Server |
| `job_itens_pedido.json` | Definição do Job 04 → 05 → 06 | — | — |

Cada notebook abre com a lista das regras ou dos KPIs que implementa. Todos carregam o `00` (direto ou pelo `01`) com `%run`.

## Como executar

1. Na raiz do repositório no Workspace, copiar `.env.example` para `.env` e preencher
   `ADLS_STORAGE_ACCOUNT`, `ADLS_CLIENT_ID`, `ADLS_TENANT_ID`, `ADLS_CLIENT_SECRET` e, para `03`/`06`,
   `JDBC_HOSTNAME`, `JDBC_DATABASE`, `JDBC_USERNAME`, `JDBC_PASSWORD`. O `00` localiza o arquivo sozinho.
2. Garantir que a **Silver de pedidos** existe em `squad2/silver/ecommerce_pedidos` (com `id_pedido` e
   `dt_pedido`) e a **Silver de produtos** em `squad2/silver/ecommerce_produtos` (com `sku` e `preco_lista`).
   Se estiverem em outro lugar, trocar `CAMINHO_SILVER_PEDIDOS` / `CAMINHO_SILVER_PRODUTOS` no `00`.
3. Rodar `04` → `05` → `06`, nessa ordem, **depois** da Silver de pedidos do mesmo ciclo.
4. Com a janela de tempo real aberta, o `04` fica em polling a cada 5 s e para sozinho após 30 min de execução
   ou 10 min sem arquivo novo. Com `loop_ativo = false`, faz só uma passada.
5. Agendado: importar `job_itens_pedido.json` (trocar `SEU_USUARIO`). Roda a cada 5 min, das 8h às 12h55, em
   dias úteis, passa `loop_ativo=false` para a Bronze e é criado **pausado**.

## Bronze (`04`)

| # | Regra |
|---|---|
| B1 | Só entra arquivo que ainda não está na Bronze (comparação por `bronze_source_file`). Reexecutar não duplica. |
| B2 | Nenhuma transformação nas colunas de origem; leitura sempre com `SCHEMA_ORIGEM`. |
| B3 | `bronze_ingested_at` = momento da leitura. |
| B4 | `bronze_source_file` = caminho relativo ao container, sem o nome da storage account. |
| B5 | Partição `ano/mes/dia/hora` da ingestão, horário de Brasília (a tabela não tem data de negócio). |
| B6 | Gravação só em `append`. |
| B7 | O loop sempre termina (teto de duração e parada por ociosidade). |

## Silver (`05`)

Lê da Bronze só o que passou do watermark (`bronze_ingested_at` máximo entre Silver, quarentena e espera),
junta os itens que estavam esperando o pedido e marca cada regra numa coluna `flag_*`.

| Regra | Coluna | Critério | Destino |
|---|---|---|---|
| T1 | `flag_schema` | coluna obrigatória ausente no arquivo ou nula na linha | quarentena |
| T2 | `flag_pedido_reprovado` | `id_pedido` está na quarentena de pedidos | quarentena |
| T2 | `flag_sem_pedido` | `id_pedido` não está nem na Silver nem na quarentena de pedidos | espera; quarentena após 24 h |
| T3 | `flag_quantidade` | `quantidade` nula ou ≤ 0 | quarentena |
| T4 | `flag_preco` | `preco_unitario` ≤ 0 ou mais de 60% abaixo do `preco_lista` | quarentena |
| T5 | `flag_duplicado` | `id_item_pedido` repetido no lote ou já na Silver | quarentena |
| N9 | `flag_desconto` | `desconto_aplicado` > `preco_unitario` | quarentena |
| extra | `flag_desconto_negativo` | `desconto_aplicado` < 0 | quarentena |

A Silver acrescenta `dt_pedido` (da Silver de pedidos), `receita_bruta` e `receita_liquida`, e é particionada por
ano e mês de `dt_pedido`. A seção **Insights** mostra as violações por regra em gráfico.

## Gold (`06`)

KPIs recalculados inteiros a cada execução (`overwrite`); `gold_hist_kpis_itens_pedido` guarda uma foto por execução
(`append`). Cada tabela vai para Delta (`gold/`) e para o SQL Server (`squad2.*`).

| Regra | Tabela | Alerta |
|---|---|---|
| N6 | `gold_kpi_skus_unicos_1h` | SKUs únicos na hora caem mais de 50% frente à média das 6 horas anteriores |
| N7 | `gold_kpi_top5_skus_30min` | SKU líder concentra mais de 30% das unidades da janela |
| N8 | `gold_kpi_receita_itens_pedido_1h` | desconto acima de 25% da receita bruta |
| N9 | (Silver) | contado em `gold_qualidade_itens_pedido` |
| N10 | `gold_kpi_itens_por_pedido_1h` | menos de 3 itens por pedido na hora |

Para o dashboard: `gold_fato_itens_pedido` (um item válido por linha, com data, hora, dia da semana, faixa de preço e
desconto) e `gold_qualidade_itens_pedido` (dia de ingestão × regra, com aprovadas, barradas e aguardando).

Fora de uma janela de tempo real, o instante de referência dos KPIs passa a ser o último pedido da Silver, e o
notebook avisa.

## Decisões técnicas

- **Spark do começo ao fim.** Leitura e escrita por `spark.read`/`write`; gráficos só sobre resultados agregados.
- **Credenciais por chamada.** O Serverless bloqueia `spark.conf.set` para `fs.azure.*`; elas vão em
  `.options(**adls_options)` em cada leitura e escrita do lake. Nenhuma credencial no código.
- **Tabelas de apoio pela Silver.** Pedidos e produtos são lidos da Silver deles, nunca do `raw`.
- **Espera só para quem pode chegar.** Item de pedido reprovado vai direto para a quarentena (`PEDIDO_REPROVADO`).
  Item cujo pedido não está em lugar nenhum pode ser defasagem entre notebooks: fica em `waiting/` e é retentado
  a cada execução por até 24 h.
- **Reprocessamento.** O widget `reprocessar = true` do `05` relê a Bronze inteira e regrava Silver, quarentena e
  espera de itens, para aplicar uma regra nova ao histórico sem apagar pasta à mão.
- **Data de negócio emprestada.** Itens não têm data; a Silver herda `dt_pedido` do pedido para particionar e
  para as janelas dos KPIs.
- **SQL Server pelo conector nativo `sqlserver`.** O JDBC genérico é bloqueado para escrita no Serverless.

## Achados de qualidade

Lotes de 21/09 a 08/10/2026: 353 lotes, 13.325 itens, 2.222 pedidos, 3.038 SKUs.

- 469 itens com quantidade zero (3,5%);
- 191 itens com preço zero ou negativo (mínimo −R$ 299,99);
- cerca de 1.500 itens com preço mais de 60% abaixo do catálogo (11%);
- 634 itens com desconto maior que o preço unitário (4,8%) e 11 com desconto negativo;
- nenhum nulo, nenhuma chave repetida e todos os itens com pedido correspondente, no mesmo lote.

## Pendências

- Limiares de N6 (queda de 50%) e N7 (concentração de 30%) são provisórios até a confirmação do negócio.
- Confirmar onde ficam as Silvers de pedidos e de produtos usadas por T2 e T4.
- Revisar os "Resumo" do `05` e do `06` quando houver novas janelas (escritos com a execução de 13/10).
