# P3: estimativa do valor do pedido

## O problema

Estimar o valor total de um pedido a partir do contexto da compra: hora, dia da semana, número de itens,
categoria dominante, método de pagamento e UF do endereço. O modelo pedido é o LightGBM Regressor,
avaliado por MAE e R² contra um baseline simples, o ticket médio por categoria dominante.

## Dados

Usamos duas fontes, só para leitura. O histórico (CSV do batch) tem 90.335 pedidos, de jan/2024 a ago/2026.
O tempo real vem das Silvers de pedidos, itens, endereços, produtos e categorias: 1.834 pedidos com itens,
em 8 dias. O histórico entrou porque o tempo real sozinho tem pouco volume para treinar.

## O que a análise exploratória mostrou

O valor do pedido é exatamente a soma dos itens (correlação de 1,000 no histórico). Por isso nenhuma
coluna em reais do carrinho entra no modelo, porque ela entregaria a resposta.

O que explica o ticket é o tamanho do carrinho. A quantidade total de unidades tem correlação de 0,81 com o
ticket no histórico. Hora, dia da semana, pagamento e UF quase não mexem no valor: o ticket médio por
método ou por UF fica entre R$ 830 e R$ 844.

As duas fontes são bem diferentes. No histórico o pedido médio é de R$ 842 com 21,6 itens. No tempo real,
R$ 274 com 5,1 itens. O tempo real também traz valores que o histórico não tem, como cartão de débito e a
categoria Mercearia Salgada.

Achamos 88 pedidos no tempo real (4,8%) com algum item de preço 5 vezes acima do preço de lista. Eles somam
22,1% da receita e são tratados à parte nas métricas.

A UF do endereço só aparecia em 19,4% dos pedidos do tempo real. Incluindo a quarentena de endereços (que
tem UF válida) e, quando faltava, usando a UF do cadastro do cliente, a cobertura chegou a 99,6%. No
histórico, onde a UF real é conhecida, essa regra acertou 100% dos casos em que se aplica.

## Engenharia de features

O modelo usa 12 entradas, uma linha por pedido: hora, dia da semana, período, fim de semana, número de
itens, quantidade total, número de categorias, quantidade por item, categoria dominante, método de
pagamento, UF e uma flag de UF informada. As categóricas viram índices inteiros. Não fizemos normalização
nem PCA, porque modelos de árvore não dependem de escala e, com 12 colunas, a redução atrapalharia a
leitura da importância das variáveis. O treino usa o log do ticket, e as métricas voltam para reais.

## Baseline do modelo

| Cenário | MAE modelo | MAE baseline | R² modelo |
|---|---|---|---|
| Treino e teste no histórico | R$ 122 | R$ 312 | 0,82 |
| Treino no histórico, teste no tempo real | R$ 151 | R$ 480 | 0,08 |
| Treino e teste no tempo real | R$ 137 | R$ 191 | 0,04 |
| Tempo real sem os pedidos fora da curva | R$ 85 | R$ 126 | 0,44 |

O modelo erra menos que o baseline em todos os cenários. No histórico o erro cai 61%. A quantidade total
responde por cerca de 76% da importância, seguida da categoria dominante e do número de itens, e o SHAP
confirma essa ordem. O modelo fica gravado no lake e é relido para pontuar pedidos novos.

## Segunda técnica: CatBoost

Rodamos o CatBoost com as mesmas 12 entradas e nos mesmos cenários. Os dois empatam na configuração final
(MAE de R$ 122,6 do LightGBM contra R$ 122,7 do CatBoost, empate pelo bootstrap). O CatBoost só ganha com folga quando treina com
pouco dado, só no tempo real. A regra de desempate foi fixada antes de medir: o mais rápido ganha. Ficou o
LightGBM, que treina mais de 10 vezes mais rápido.

## Evolução do modelo

Todas as versões foram medidas no mesmo teste, os 367 pedidos mais recentes do tempo real.

| Versão | O que muda | MAE | MAE sem fora da curva | R² sem fora da curva |
|---|---|---|---|---|
| Baseline | média por categoria | R$ 191 | R$ 148 | −0,17 |
| v1 | treino só no histórico | R$ 151 | R$ 108 | 0,32 |
| v2 | v1 com correção de nível | R$ 140 | R$ 95 | 0,36 |
| v3 | treino histórico + tempo real | R$ 126 | R$ 80 | 0,48 |
| v4 | v3 sem os pedidos fora da curva no treino | R$ 123 | R$ 76 | 0,49 |

O v4 erra 19% menos que a primeira versão e 36% menos que o baseline. Ele também ficou em primeiro numa
validação separada do teste, então a escolha não dependeu do conjunto de teste. É a versão gravada para
pontuar o tempo real.

## Validação por dia e por período

Quase todo o teste do tempo real é de um dia, 08/10. Esse dia não é atípico no valor: o ticket médio foi de
R$ 269, contra R$ 266 a R$ 302 nos outros dias com volume. A diferença é que 75% dos pedidos dele foram à
tarde.

Para não depender de um dia só, repetimos a comparação dia a dia em 03, 05, 07 e 08/10. Cada dia é testado
com um modelo treinado no histórico e no tempo real até a véspera. O v4 ganha do baseline nos quatro dias e
do v1 em três, com empate no outro. Contra um modelo treinado só no tempo real, ganha em dois dias e empata
em dois. Somando os 1.113 pedidos dos quatro dias, o erro médio é de R$ 113 no v4, R$ 121 no modelo só do
tempo real, R$ 138 no v1 e R$ 186 no baseline.

O erro é maior à tarde. Sem os pedidos fora da curva, o v4 erra R$ 55 por pedido de manhã e R$ 80 à tarde,
período em que o ticket médio também é maior (R$ 234 contra R$ 208).

## Limites

O modelo estima bem o pedido típico. Os pedidos com preço fora da curva concentram quase todo o erro
quadrático, e nenhuma variável de contexto prevê esse preço. Por isso a evolução é medida pelo MAE e pelo R²
dos pedidos normais. O modelo também prevê um pouco abaixo do valor real, então não serve para projetar
receita somando previsões.
