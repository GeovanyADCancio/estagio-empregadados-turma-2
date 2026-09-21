# Squad 2 - Real Time for Business

## Análise Exploratória - `ecommerce_pedidos`

Projeto desenvolvido durante o estágio de Engenharia de Dados com objetivo de validar o fluxo de dados ponta a ponta utilizando Azure Data Lake Storage Gen2, Databricks, análise exploratória e SQL Server.

---

# 👥 Equipe

**Squad:** 2 - Real Time for Business  
**Dupla:** 3

**Integrantes:**

- Eduardo Geraldo de Souza
- Aliedson Soares Silva

---

# 🎯 Objetivo

Realizar o fluxo completo da tabela `ecommerce_pedidos`:

- Conexão com Azure Data Lake Storage Gen2;
- Leitura dos dados;
- Análise exploratória;
- Validação da qualidade dos dados;
- Geração de indicadores;
- Persistência no SQL Server.

---

# 🗂️ Dados analisados

**Tabela:**

```
ecommerce_pedidos
```

**Origem:**

```
Azure Data Lake Storage Gen2
```

**Container:**

```
raw
```

**Arquivo:**

```
batch-data/ecommerce_pedidos.csv
```

---

# 🏗️ Fluxo do Projeto

```
Azure Data Lake Gen2

        ↓

ecommerce_pedidos.csv

        ↓

Databricks

        ↓

Análise Exploratória

        ↓

SQL Server

        ↓

squad2.ecommerce_pedidos
```

---

# 🛠️ Tecnologias

- Python
- Pandas
- Databricks
- Azure Data Lake Storage Gen2
- SQL Server Azure
- Git/GitHub

---

# 📁 Estrutura

```
RealTimeBusiness_Squad2_Dupla3

├── notebooks
│   ├── 01_conexao_adls_gen2.ipynb
│   └── 02_eda_pedidos.ipynb
│
├── src
│   └── salvar_pedidos_sql.py
│
├── README.md
└── .gitignore
```

---

# 🔎 Análise Exploratória

Foram realizadas análises de:

- estrutura dos dados;
- valores nulos;
- registros duplicados;
- evolução mensal dos pedidos;
- faturamento;
- ticket médio;
- status dos pedidos;
- métodos de pagamento;
- taxa de cancelamento.

---

# 📊 Principais Resultados

- **Registros analisados:** 90.335
- **Colunas:** 10
- **Valores nulos:** nenhum encontrado
- **Duplicados:** nenhum encontrado
- **Ticket médio:** R$ 842,06
- **Taxa de cancelamento:** 5,04%

---

# 🗄️ SQL Server

Após a análise, os dados foram carregados na tabela:

```sql
squad2.ecommerce_pedidos
```

Validação:

```sql
SELECT COUNT(*)
FROM squad2.ecommerce_pedidos;
```

Resultado:

```
90.335 registros
```

---

# 🔐 Segurança

As credenciais são armazenadas no arquivo `.env` e não são versionadas no GitHub.

Variáveis utilizadas:

```
ADLS_CLIENT_ID
ADLS_TENANT_ID
ADLS_CLIENT_SECRET
STORAGE_ACCOUNT_NAME
CONTAINER_NAME
SQL_HOST
SQL_DATABASE
SQL_USERNAME
SQL_PASSWORD
```

---

# ✅ Conclusão

O projeto validou o fluxo completo de dados:

Azure Data Lake → Databricks → Análise Exploratória → SQL Server

A entrega demonstra a integração entre armazenamento, processamento e análise de dados da tabela `ecommerce_pedidos`.

---

**Squad 2 - Real Time for Business**  
**Dupla 3**  
Eduardo Geraldo de Souza  
Aliedson Soares Silva