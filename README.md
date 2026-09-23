# 🚀 Squad 2 - Real Time for Business

# Análise Exploratória de Dados - `ecommerce_pedidos`

Projeto desenvolvido durante o estágio de Engenharia de Dados com objetivo de validar o fluxo completo de dados, desde a ingestão no Azure Data Lake Storage Gen2 até a disponibilização dos dados para análise no SQL Server.

O projeto contempla:

- carregamento de dados históricos;
- processamento de eventos em tempo real;
- análise exploratória dos dados;
- validação da qualidade;
- geração de indicadores de negócio;
- persistência da tabela consolidada.

---

# 👥 Equipe

**Squad:** 2 - Real Time for Business

**Dupla:** 3

**Integrantes:**

- Eduardo Geraldo de Souza
- Aliedson Soares Silva

---

# 🎯 Objetivo

Realizar a análise exploratória da tabela:

```
ecommerce_pedidos
```

validando a qualidade dos dados e criando uma visão consolidada entre:

- dados históricos (Batch);
- novos eventos recebidos em tempo real (Real Time).

---

# 🏗️ Arquitetura do Fluxo de Dados

```
Azure Data Lake Storage Gen2

          |
          |
   ----------------
   |              |
 Batch         Real Time

 CSV            Parquet

   |              |
   ----------------

          |

      Databricks

          |

  Análise Exploratória

          |

    SQL Server Azure

          |

 squad2.ecommerce_pedidos
```

---

# 📂 Fontes de Dados

## 📦 Histórico Batch

Arquivo utilizado:

```
batch-data/ecommerce_pedidos.csv
```

Características:

- formato: CSV;
- dados acumulados;
- utilizado para análises históricas.

Quantidade analisada:

```
90.335 registros
```

---

## ⚡ Eventos Real Time

Arquivo utilizado:

```
real-time-data/*/ecommerce_pedidos.parquet
```

Características:

- formato: Parquet;
- representa novas cargas recebidas;
- utilizado para validação dos eventos recentes.

Quantidade analisada:

```
6 registros
```

---

# 🔎 Análises Realizadas

## Estrutura dos Dados

Foram avaliados:

- quantidade de registros;
- quantidade de colunas;
- tipos dos dados;
- estrutura das informações.

---

# 🧪 Qualidade dos Dados

Foram realizadas validações:

## Valores Nulos

Resultado:

✅ Nenhum valor nulo identificado.

---

## Registros Duplicados

Resultado:

✅ Nenhum registro duplicado encontrado.

---

## Integridade da Chave Primária

Campo analisado:

```
id_pedido
```

Resultado:

- 90.335 registros analisados;
- 90.335 pedidos únicos;
- nenhuma duplicidade encontrada.

---

# 📊 Análises de Negócio

## Evolução dos Pedidos

Análise do volume de pedidos ao longo do tempo.

Objetivo:

- identificar crescimento;
- analisar sazonalidade;
- observar variações mensais.

---

## Faturamento Mensal

Análise da receita gerada pelos pedidos.

Indicador:

```
SUM(valor_total)
```

---

## Ticket Médio

Cálculo:

```
Faturamento Total / Quantidade de Pedidos
```

Objetivo:

Avaliar o comportamento médio de consumo dos clientes.

---

## Status dos Pedidos

Análise operacional:

- Entregue;
- Enviado;
- Em Separação;
- Cancelado;
- Pagamento Aprovado;
- Processando.

---

## Métodos de Pagamento

Foram analisados:

- quantidade de pedidos por método;
- faturamento por método.

Métodos encontrados:

- Cartão de Crédito;
- Pix;
- Boleto.

---

# ⚡ Análise dos Eventos Real Time

A janela de dados recebida foi analisada considerando:

- quantidade de novos pedidos;
- valor movimentado;
- status dos pedidos;
- métodos de pagamento.

Resultado da janela analisada:

```
6 novos pedidos

R$ 754,66 movimentados
```

---

# 🔄 Consolidação Batch + Real Time

Após análise, os dados foram consolidados:

```
Histórico Batch

90.335 registros

+

Eventos Real Time

6 registros

=

Tabela Consolidada

90.341 registros
```

Validação realizada:

```
Pedidos únicos: 90.341

Duplicidades: 0
```

---

# 🗄️ Persistência SQL Server

Após a validação, os dados consolidados foram enviados para:

```sql
squad2.ecommerce_pedidos
```

A tabela contém:

- histórico completo;
- novos eventos recebidos;
- registros validados sem duplicidade.

---

# 🛠️ Tecnologias Utilizadas

- Python
- Pandas
- PySpark
- Databricks
- Azure Data Lake Storage Gen2
- SQL Server Azure
- DBeaver
- Git/GitHub

---

# 🔐 Segurança

As credenciais de acesso são carregadas utilizando variáveis de ambiente através do arquivo:

```
.env
```

O arquivo `.env` não deve ser versionado no GitHub.

Configuração obrigatória no `.gitignore`:

```
.env
.env.*
*.env
```

---

# ✅ Resultado Final

O projeto validou o fluxo completo:

```
Azure Data Lake

        ↓

Databricks

        ↓

Análise Exploratória

        ↓

Consolidação Batch + Real Time

        ↓

SQL Server

        ↓

squad2.ecommerce_pedidos
```

A tabela final está preparada para consumo analítico e geração de indicadores de negócio.