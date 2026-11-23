# ML P3: estimativa do valor do pedido (para revisão)

Estado em 22/11/2026. Os notebooks 07 a 10 estão escritos e rodaram inteiros, sem erro, em Spark 4.2 com Delta,
sobre uma cópia dos dados do lake. Ainda não rodaram no Databricks. Estão na branch
`feat/squad2-fase2-ml-jose-tiago`, que tem só os arquivos desta fase e o `00_setup_config`.

## O problema

Estimar o `valor_total` do pedido (ticket) a partir do contexto da compra: hora, dia da semana, nº de itens,
categoria dominante, método de pagamento e UF do endereço. A planilha da squad pede LightGBM Regressor,
explicado por importância de variáveis ou SHAP, com MAE e R² comparados ao baseline "ticket médio por
categoria dominante".

## Cards do Trello

A dupla tem seis cards nesta etapa. Os seis têm entrega escrita; nenhum foi executado no Databricks ainda.

| Card | O que pede | Onde está |
|---|---|---|
| Análise exploratória dos dados | saber se os dados servem, conhecer as tabelas e tirar insights sobre o que entra no modelo | 07, seções "EDA 1" a "EDA 6" e "Resumo": cobertura, nulos e duplicados, vazamento, distribuição do ticket, itens fora da curva, correlação, categóricas |
| Engenharia de Features | preparar os dados e criar colunas numéricas que ajudem a achar padrões | 07, seções "Uma linha por pedido", "Engenharia de features" e "Gravação": 12 entradas, das quais `n_itens`, `qtd_total`, `n_categorias`, `qtd_por_item`, `fim_de_semana`, `periodo`, `uf_informada` e os quatro índices das categóricas são derivadas |
| Proposta de modelo [One Page] | um slide com problema, tabelas, colunas de entrada e características do modelo | `P3_proposta_modelo_onepage.pptx`, com os quatro blocos numerados de 1 a 4 |
| Baseline do modelo treinado e funcional | primeira versão do modelo funcionando, para melhorar depois | 08: LightGBM treinado, comparado ao baseline, explicado, salvo no lake e relido para pontuar 5 pedidos |
| Aplicação de segunda técnica de ML | CatBoost nos mesmos dados do LightGBM e dizer qual é melhor | 09: os dois modelos nos cenários do 08 e num cenário e (configuração de produção), com bootstrap e tempo de treino |
| Evolução do baseline | mostrar com resultados que o modelo melhorou | 10: versões v1 a v4 medidas no mesmo teste do tempo real, contra o baseline e o modelo c do 08, com bootstrap; v4 gravado como versão 2 |

A EDA e a engenharia de features estão no mesmo notebook, em seções separadas, porque a segunda usa a base
montada pela primeira. A correlação fica na parte de EDA. Se for preferível um notebook por card, o corte é na
seção "Engenharia de features" do 07.

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `squad2_itens_pedidos/00_setup_config.ipynb` | credenciais e caminhos; o 07 e o 08 chamam por `%run` |
| `squad2_itens_pedidos/07_ml_eda_features_valor_pedido.ipynb` | EDA e engenharia de features; grava a tabela de features |
| `squad2_itens_pedidos/08_ml_baseline_valor_pedido.ipynb` | baseline, LightGBM, SHAP, modelo salvo e pontuação de teste |
| `squad2_itens_pedidos/09_ml_catboost_valor_pedido.ipynb` | CatBoost × LightGBM nos mesmos dados |
| `squad2_itens_pedidos/10_ml_evolucao_valor_pedido.ipynb` | evolução v1 → v4 e gravação do v4 |
| `P3_proposta_modelo_onepage.pptx` e `.png` (raiz, fora do git) | slide do card "Proposta de modelo [One Page]" |

O slide fica fora do git.

## Fontes

| Fonte | Caminho | Motivo |
|---|---|---|
| Histórico | `raw/batch-data/*.csv` (pedidos, itens, endereços, produtos, categorias) | o tempo real tem 8 dias; o histórico tem 90 mil pedidos no mesmo layout |
| Tempo real | Silvers `squad2/silver/{ecommerce_pedidos,ecommerce_enderecos,ecommerce_produtos,ecommerce_categorias}`, `squad2/quarantine/ecommerce_enderecos` e `squad2/grupo2/silver/ecommerce_itens_pedido` | é o dado que o modelo vai pontuar; tabela de outra equipe é lida pela Silver, nunca pelo raw |

Tudo é só leitura. O histórico é lido com schema declarado, então se o cabeçalho de um CSV mudar a leitura
falha em vez de deslocar colunas. As datas do tempo real passam para o fuso de São Paulo, e "hora" significa a
mesma coisa nas duas fontes.

Saídas:
- `squad2/grupo2/ml/features_valor_pedido` (Delta, sobrescrita a cada execução do 07): uma linha por pedido,
  partição `fonte` (`historico`/`stream`) e coluna `conjunto` (treino/teste, 80/20 por data dentro de cada fonte).
- `squad2/grupo2/ml/modelo_valor_pedido` (Delta, uma versão nova a cada execução do 08 ou do 10): métricas,
  parâmetros e o modelo em texto. A coluna `cenario` diz em qual teste as métricas foram medidas.

## Achados da EDA

| Achado | Histórico | Tempo real |
|---|---|---|
| Pedidos na base (com itens) | 90.335, jan/2024 a ago/2026 | 1.834 de 2.167, em 8 dias |
| Pedidos com UF | 100% | 99,6% (71,0% pelo endereço, 28,6% pelo cadastro do cliente) |
| Correlação ticket × soma dos itens | 1,000 | 0,998 |
| Ticket médio / mediana | R$ 842 / R$ 786 | R$ 274 / R$ 172 |
| Itens por pedido | 21,6 | 5,1 |
| Pedidos com item ≥ 5× o preço de lista | 0 | 88 (4,8%), com 22,1% da receita |

O `valor_total` é a soma dos itens, então qualquer coluna em reais do carrinho entregaria a resposta ao modelo.
Nenhuma entra.

O ticket acompanha o tamanho do carrinho. `qtd_total` tem r = 0,81 no histórico e 0,38 no tempo real; `n_itens`,
0,80 e 0,34; `n_categorias`, 0,65 e 0,18. A categoria dominante também separa: no histórico, Matinais, Doces &
Biscoitos tem ticket médio de R$ 1.081 e Adega & Destilados, de R$ 428. Hora, dia da semana e fim de semana têm
|r| < 0,1 nas duas fontes, e no histórico o ticket médio por método de pagamento ou por UF fica entre R$ 830 e
R$ 844.

No histórico o pedido é 3 vezes maior e tem 4 vezes mais itens. No tempo real quase tudo cai de manhã ou à tarde,
horário em que o gerador de dados fica ligado. `cartão débito` (511 pedidos, 28%) e a categoria Mercearia
Salgada (55 pedidos) só existem no tempo real. Os 88 pedidos com item fora da curva têm ticket médio de
R$ 1.264, contra R$ 224 nos demais.

## UF do endereço

Em todas as fontes, toda linha de endereço tem UF válida. O que falta é o pedido achar a sua linha de
endereço, e o 07 faz isso em dois passos.

1. Endereço do pedido. A quarentena de endereços entra junto com a Silver. Ela barra por CEP fora do padrão
   (3.011 linhas) ou cadastro incompleto (1.630), mas a UF está sempre preenchida, e com isso a cobertura sobe
   de 19,4% para 71,0%. O join exige endereço e cliente iguais. Os 531 pedidos que sobram têm id de endereço
   pequeno, que coincide com ids do histórico, só que de outro cliente.
2. Cadastro do cliente. Esses 531 pedidos também têm `id_cliente` na faixa do histórico (101 a 9.995), e todos
   esses clientes têm endereço no CSV histórico. Se o pedido não acha o endereço, a UF vem dos endereços
   cadastrados do cliente, desde que estejam todos na mesma UF. No histórico, onde a UF real é conhecida, a
   regra se aplica a 97,5% dos pedidos e acerta 100% deles. Ela cobre 524 dos 531; os 7 de clientes com
   endereços em duas UFs ficam `NAO_INFORMADO`.

A cobertura no tempo real passa de 71,0% para 99,6%. A coluna `uf_origem` (`endereco`, `cliente`, `sem`) diz de
onde veio cada UF. Entre os pedidos com UF, a proporção é SP 40%, RJ 34%, MG 19% e ES 7%, a mesma do histórico.

Latitude e longitude não resolvem esses casos. Elas só existem na tabela de endereços, que já tem a UF, e o
pedido sem endereço também fica sem coordenada. O rastreamento não tem localização (só status, data e
observação). As coordenadas serviriam para conferir se a UF cadastrada bate com o ponto no mapa.

O passo 2 assume que o cliente 101 do tempo real é o mesmo cliente 101 do histórico. A UF desses 524 pedidos é
uma inferência a partir do cadastro, e por isso `uf_origem` fica na tabela.

## Colunas

| Coluna | Entra? | Motivo |
|---|---|---|
| `hora`, `dia_semana`, `periodo`, `fim_de_semana` | sim | contexto previsto no problema; `periodo` e `fim_de_semana` saem de `dt_pedido` |
| `n_itens`, `qtd_total`, `n_categorias`, `qtd_por_item` | sim | tamanho e variedade do carrinho, sem preço |
| `categoria_dominante` | sim | perfil do carrinho e chave do baseline |
| `metodo_pagamento` | sim | contexto previsto; o histórico é normalizado para o formato do tempo real |
| `uf` + `uf_informada` | sim | contexto previsto; a flag separa UF desconhecida de UF real |
| `uf_origem` | não como entrada | auditoria: de onde veio a UF |
| `flag_item_fora_curva` | não como entrada | fica na tabela para filtrar ou analisar o erro |
| soma, preço médio ou máximo dos itens, `desconto_aplicado` | não | vazamento (ticket = soma dos itens) |
| `valor_frete`, `status_pedido`, `dt_previsao_entrega`, `dt_ultima_atualizacao_status` | não | só existem depois do pedido ou dependem do ticket |
| `id_*`, `cidade`, `latitude`, `longitude`, `apelido` | não | identificadores; a localização fica resumida na UF |

São 12 entradas. As categóricas viram índice inteiro, ordenado pela frequência no histórico; valor que só
aparece no tempo real vai para o fim. O LightGBM recebe esse índice como categórica, sem one-hot. Não há
normalização, porque árvore não depende de escala, nem PCA, porque com 12 colunas a redução apagaria a leitura
do SHAP. O treino usa o log do ticket e as métricas voltam para reais antes da comparação com o baseline.

## Baseline (08)

LightGBM com 400 árvores, taxa de aprendizado 0,05, 31 folhas e subsample 0,8. Corte 80/20 por data.

| Cenário | MAE modelo | MAE baseline | R² modelo | R² baseline |
|---|---|---|---|---|
| a: treino e teste no histórico | R$ 122 | R$ 312 | 0,82 | 0,25 |
| b: treino no histórico, teste no tempo real | R$ 149 | R$ 480 | 0,08 | −0,82 |
| c: treino e teste no tempo real | R$ 137 | R$ 191 | 0,04 | 0,00 |
| d: igual ao c, sem os pedidos com item fora da curva | R$ 85 | R$ 126 | 0,44 | 0,06 |

O modelo erra menos que o baseline nos quatro cenários. No histórico o erro médio cai 61% e o modelo explica
82% da variação do ticket. `qtd_total` responde por cerca de 76% da importância (gain), seguida de categoria
dominante (12%) e nº de itens (7%); hora, pagamento e UF quase não pesam, e o SHAP mostra a mesma ordem.

No tempo real o R² cai. O modelo do histórico erra o nível do ticket (R$ 840 contra R$ 275), e os 88 pedidos
com preço fora da curva concentram boa parte do erro: sem eles o R² do cenário c sobe de 0,04 para 0,44.

O cenário a foi salvo como versão 1 em `modelo_valor_pedido`. O Spark não grava modelo com `adls_options`, então
o modelo vai como texto numa tabela Delta comum. O 08 relê essa tabela, pontua 5 pedidos do tempo real e confere
que a previsão é igual à do modelo em memória.

Estes números vêm da cópia local dos dados. Depois de rodar no Databricks, conferir o resumo do 08.

## Segunda técnica: CatBoost (09)

O CatBoost recebe as mesmas 12 colunas, com os quatro índices das categóricas declarados como `cat_features`.
Os dois modelos rodam nos cenários a, b, c e d do 08 e num cenário e: treino histórico + stream sem os pedidos
com item fora da curva, com a coluna `eh_stream`, testado no stream. O e é a configuração que vai para produção
(o v4 do notebook 10), e a regra de decisão foi fixada antes: ganha o menor MAE no e; se o bootstrap pareado
der empate, ganha o que treina mais rápido.

| Cenário | MAE LightGBM | MAE CatBoost | Resultado do bootstrap |
|---|---|---|---|
| a: histórico → histórico | R$ 122,4 | R$ 122,4 | empate |
| b: histórico → stream | R$ 149,4 | R$ 150,9 | empate |
| c: stream → stream | R$ 137,0 | R$ 127,1 | CatBoost melhor |
| d: stream sem fora da curva | R$ 84,6 | R$ 81,2 | empate |
| e: histórico + stream sem fora da curva → stream | R$ 122,8 | R$ 122,5 | empate |

Os dois empatam no erro e, pela regra de decisão, fica o LightGBM, que treina mais de 10 vezes mais rápido. O
CatBoost só ganha com folga no c, que tem 1.467 pedidos de treino; no d ele também erra menos, mas o bootstrap
dá empate. A explicação provável é que ele regulariza melhor com amostra pequena, mas não testamos qual mecanismo
responde por isso. Quando o histórico entra no treino, a vantagem some. A ordem das variáveis é a mesma nos dois (`qtd_total`, categoria
dominante, nº de itens).

## Evolução do baseline (10)

Todas as versões são medidas no mesmo teste: os 367 pedidos mais recentes do stream. As referências são o
baseline (média do ticket por categoria no treino do stream) e o modelo c do 08, treinado só no stream.

| Versão | O que muda | MAE todos | MAE sem fora da curva | R² sem fora da curva | Erro ≤ R$ 50 |
|---|---|---|---|---|---|
| baseline | média por categoria | R$ 191 | R$ 148 | −0,17 | 23% |
| c do 08 | LightGBM só no stream | R$ 137 | R$ 90 | 0,41 | 46% |
| v1 | LightGBM só no histórico (08) | R$ 149 | R$ 106 | 0,34 | 37% |
| v2 | v1 + correção de nível | R$ 139 | R$ 95 | 0,37 | 43% |
| v3 | treino histórico + stream com `eh_stream` | R$ 126 | R$ 80 | 0,48 | 51% |
| v4 | v3 sem os pedidos fora da curva no treino | R$ 123 | R$ 77 | 0,49 | 57% |

O v4 erra 18% menos que o v1 (queda de R$ 27, intervalo de 95% de R$ 19 a R$ 34), 36% menos que o baseline e
10% menos que o modelo c (R$ 14, intervalo de R$ 9 a R$ 19). Numa validação separada (os 20% mais recentes do
treino do stream, sem tocar no teste), o v4 também é o melhor, então a escolha não dependeu do
teste. O R² no teste do histórico continua 0,82.

O R² de todos os pedidos fica entre 0,06 e 0,08 em todas as versões; o do v1 é o maior, porque ele prevê valores
mais altos e chega mais perto dos pedidos gigantes. Os 15 pedidos do teste com item fora da curva (4,1%)
concentram 92% do erro quadrático do v4, e nenhuma variável de contexto prevê esse preço. Por isso a evolução é
medida pelo MAE e pelo R² dos pedidos normais.

Dois limites que a apresentação precisa dizer. O teste do tempo real cobre praticamente um dia: 346 dos 367
pedidos são de 08/10. E o modelo prevê abaixo do valor real: somadas, as previsões ficam 25% abaixo da receita do
teste (10% nos pedidos normais), porque o treino em log mira o valor típico. O modelo estima o pedido típico; não
serve para projetar receita somando previsões.

A validação do 09 e do 10 foi refeita fora dos notebooks, com 5 seeds: a ordem das versões e o empate do cenário e
se mantêm em todas.

O v4 foi gravado como versão 2 em `modelo_valor_pedido`. Para pontuar, o micro-lote recebe `eh_stream = 1`. As
métricas da versão 1 (08) são do teste do histórico e as da versão 2, do teste do stream; o campo `cenario`
separa as duas.

## Decisões que precisam do seu aval

1. Os 88 pedidos com preço fora da curva concentram 22,1% da receita do tempo real. As opções são tirá-los do
   treino e do teste, mantê-los e reportar a métrica com e sem eles, ou tratar como problema de qualidade da
   Silver de itens (a regra T4 só barra preço baixo). Proposta: manter na tabela e reportar as duas métricas,
   como o 08 já faz.
2. Treinar no histórico ou no tempo real. Resolvido no 10: treinar com as duas fontes juntas (v3/v4) é melhor
   que cada uma sozinha. Confirmar se o v4 passa a ser o modelo de produção.
3. `cartão débito` e Mercearia Salgada só existem no tempo real, e o modelo do histórico nunca os viu. Dá para
   aceitar como categoria nova, juntar `cartão débito` num grupo "cartão" ou tirar o método de pagamento, que
   quase não muda o ticket.
4. O contexto quase não explica o ticket; o tamanho do carrinho, sim. O problema fala em "contexto", então a
   apresentação precisa dizer isso com clareza.

## Próximos passos

1. Rodar 07, 08, 09 e 10 no Databricks e conferir os números dos resumos.
2. Se o Serverless reclamar de `sklearn`, acrescentar `scikit-learn` no `%pip` da primeira célula do 08, 09 e 10.
3. Revisar o slide one-page e colar na apresentação compartilhada.
4. Próximas melhorias: separar os pedidos com item fora da curva antes de pontuar (alerta, não estimativa) e,
   com mais semanas de tempo real, escolher a versão numa validação separada do teste.

## Como rodar no Databricks

1. Abrir a pasta `squad2_itens_pedidos/` na branch `feat/squad2-fase2-ml-jose-tiago`; o `00_setup_config`
   precisa estar na mesma pasta.
2. O 07 chama o 00 por `%run ./00_setup_config`, que carrega as credenciais do `.env` e o `adls_options`. Não é
   preciso rodar o 00 sozinho.
3. Ordem: `07_ml_eda_features_valor_pedido`, `08_ml_baseline_valor_pedido`, `09_ml_catboost_valor_pedido` e
   `10_ml_evolucao_valor_pedido`. Cada um instala o que usa na primeira célula (`lightgbm`, `shap`, `catboost`)
   e lê a tabela de features gravada pelo 07.
4. Nenhuma tabela de origem é alterada. O 07 sobrescreve só `features_valor_pedido`; o 08 e o 10 só acrescentam
   versões em `modelo_valor_pedido`; o 09 não grava nada.
