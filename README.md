# Squad 1 — Data Quality

## Dupla 4 — `ecommerce_pedidos`

### Sobre o projeto

Este projeto faz parte de uma simulação prática de Engenharia de Dados, organizada em squads.

A Squad 1 é responsável pelo fluxo de **Data Quality em tempo real**, trabalhando com dados provenientes do e-commerce da MercaData.

A Dupla 4 atua sobre a tabela:

```text
ecommerce_pedidos
```

O objetivo é construir um pipeline capaz de ingerir os dados de pedidos, controlar o processamento incremental, aplicar regras de qualidade e disponibilizar informações confiáveis para as próximas camadas da arquitetura.

---

## Arquitetura

O projeto segue uma arquitetura em camadas:

```text
RAW
  ↓
Bronze
  ↓
Silver
  ↓
Gold
```

### RAW

Área onde os dados são recebidos originalmente.

Os pedidos chegam em micro-lotes no formato Parquet:

```text
real-time-data/
└── YYYY/
    └── MM/
        └── DD/
            └── HHMMSS/
                └── ecommerce_pedidos.parquet
```

### Bronze

Responsável pela ingestão e controle incremental dos micro-lotes.

A implementação atual utiliza:

- polling para identificação de novos arquivos;
- Manifest para controle dos arquivos já processados;
- Control Log para controle das versões dos registros;
- hash SHA-256 para identificação de versões dos pedidos;
- classificação dos registros como `NOVO`, `ATUALIZACAO` ou `DUPLICADO`;
- persistência em Delta Lake no Azure Data Lake.

### Silver

Responsável pela aplicação das regras de Data Quality sobre os dados provenientes da Bronze.

As validações são representadas por flags booleanas, permitindo identificar quais registros apresentam falhas de qualidade.

### Gold

Camada destinada à construção de métricas e informações agregadas para consumo analítico e monitoramento.

---

## Fluxo simplificado

```text
Arquivo Parquet chega no RAW
          ↓
Polling identifica novo micro-lote
          ↓
Manifest verifica se o arquivo já foi processado
          ↓
Pipeline prepara os registros
          ↓
Hash identifica a versão do pedido
          ↓
Control Log consulta o histórico
          ↓
NOVO / ATUALIZACAO / DUPLICADO
          ↓
Bronze
          ↓
Regras de Data Quality
          ↓
Silver
          ↓
Métricas de qualidade
          ↓
Gold
```

---

## Tecnologias

O projeto utiliza:

- Databricks
- Apache Spark / PySpark
- Python
- Azure Data Lake Storage Gen2
- Delta Lake
- Azure SQL
- Git
- GitHub

---

## Estrutura no Data Lake

A Squad 1 utiliza a seguinte organização:

```text
squad1/
├── bronze/
│   └── ecommerce_pedidos/
│
├── silver/
│   └── ecommerce_pedidos/
│
├── gold/
│
├── metadata/
│   ├── ecommerce_pedidos_manifest/
│   └── ingestion_control_log/
│
└── dq_monitoring_logs/
```

---

## Controle incremental

O pipeline Bronze utiliza dois mecanismos principais de controle.

### Manifest

Responsável por responder:

```text
Este arquivo já foi processado?
```

Evita o reprocessamento normal do mesmo micro-lote.

### Control Log

Responsável por responder:

```text
Esta versão deste pedido já foi processada?
```

O controle utiliza a combinação do identificador do pedido com o hash do conteúdo do registro.

---

## Versionamento por hash

O pipeline utiliza SHA-256 para identificar versões dos pedidos.

A versão atual do algoritmo é:

```python
HASH_VERSION = 2
```

O hash v2 considera os seguintes campos:

```text
id_pedido
id_cliente
id_endereco_entrega
dt_pedido
dt_previsao_entrega
status_pedido
valor_total
valor_frete
metodo_pagamento
dt_ultima_atualizacao_status
```

O versionamento do algoritmo foi adotado após a identificação de que a primeira versão não considerava `dt_previsao_entrega`.

---

## Data Quality

A camada Silver aplica regras de qualidade sobre `ecommerce_pedidos`.

Entre as validações previstas estão:

- identificação de campos obrigatórios;
- validação de domínio de status;
- validação de método de pagamento;
- validação de valores;
- consistência entre datas;
- regras relacionadas ao valor do frete;
- validações envolvendo dados de entrega e rastreamento.

Os resultados agregados das regras serão registrados na tabela compartilhada:

```text
dq_monitoring_logs
```

---

## Segurança

As credenciais utilizadas para acesso aos serviços Azure são armazenadas em **Secret Scopes do Databricks**.

Credenciais não devem ser:

- escritas diretamente nos notebooks;
- armazenadas em arquivos do projeto;
- versionadas no Git/GitHub.

---

## Documentação

A documentação técnica detalhada está organizada em:

```text
docs/
├── bronze_ecommerce_pedidos.md
├── dicionario_ecommerce_pedidos.md
├── dq_monitoring_logs.md
└── status_atual_projeto.md
```

### Conteúdo

`bronze_ecommerce_pedidos.md`  
Detalha o funcionamento da ingestão Bronze, polling, Manifest, Control Log e versionamento por hash.

`dicionario_ecommerce_pedidos.md`  
Contém o dicionário básico dos campos da tabela.

`dq_monitoring_logs.md`  
Documenta o contrato definido pela Squad 1 para monitoramento das regras de Data Quality.

`status_atual_projeto.md`  
Registra o estado atual da implementação e as pendências conhecidas.

---