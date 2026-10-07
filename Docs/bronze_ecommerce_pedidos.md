# Bronze — `ecommerce_pedidos`

## Objetivo

Ingerir os micro-lotes de `ecommerce_pedidos` provenientes da área RAW, controlar o processamento incremental e preservar o histórico de versões de negócio utilizado pelas etapas seguintes.

## Fonte

```text
real-time-data/
└── YYYY/MM/DD/HHMMSS/
    └── ecommerce_pedidos.parquet
```

Cada arquivo representa um micro-lote.

## Destino

```text
abfss://squad1@internshipdatalake.dfs.core.windows.net/bronze/ecommerce_pedidos
```

## Controles auxiliares

### Manifest

```text
abfss://squad1@internshipdatalake.dfs.core.windows.net/metadata/ecommerce_pedidos_manifest
```

Responsabilidade: registrar quais arquivos já foram processados.

### Control Log

```text
abfss://squad1@internshipdatalake.dfs.core.windows.net/metadata/ingestion_control_log
```

Responsabilidade: registrar versões conhecidas dos pedidos e apoiar a classificação de registros.

## Hash

Versão atual:

```python
HASH_VERSION = 2
```

O hash v2 considera:

- `id_pedido`
- `id_cliente`
- `id_endereco_entrega`
- `dt_pedido`
- `dt_previsao_entrega`
- `status_pedido`
- `valor_total`
- `valor_frete`
- `metodo_pagamento`
- `dt_ultima_atualizacao_status`

O hash identifica a versão do conteúdo do pedido.

## Classificação

```text
id_pedido nunca visto → NOVO
id_pedido já conhecido + hash novo → ATUALIZAÇÃO
id_pedido já conhecido + hash já existente → DUPLICADO
```

Registros `DUPLICADO` não são gravados novamente na Bronze.

## Baseline incremental

```text
real-time-data/2026/10/01/134202/ecommerce_pedidos.parquet
```

Os arquivos posteriores ao baseline são avaliados como candidatos ao processamento incremental.

## Schema atual

| Campo | Tipo |
|---|---|
| `id_pedido` | long |
| `id_cliente` | long |
| `id_endereco_entrega` | long |
| `dt_pedido` | timestamp |
| `status_pedido` | string |
| `valor_total` | double |
| `valor_frete` | double |
| `metodo_pagamento` | string |
| `dt_ultima_atualizacao_status` | timestamp |
| `data_ultima_atualizacao` | date |
| `dt_previsao_entrega` | timestamp |

## Observação sobre a natureza da Bronze

A implementação atual não é uma cópia totalmente intocada do RAW.

Ela realiza tratamentos técnicos, incluindo:

- cast de campos temporais;
- criação de `data_ultima_atualizacao`;
- cálculo de hash para controle;
- prevenção de nova gravação de versões já conhecidas.

Portanto, a Bronze atual deve ser descrita como uma camada de ingestão controlada e versionada.

## Validação do hash v2

Checkpoint conhecido da migração:

```text
Bronze física: 16038
Control Log v1: 16031 versões
Control Log v2: 16031 versões
Manifest: 25 arquivos
RAW pós-baseline ausente no v2: 0
```

Também foi realizado teste controlado alterando somente `dt_previsao_entrega`, com mudança do hash conforme esperado.

## Operação

Exemplo de polling:

```python
executar_polling(
    ciclos=120,
    intervalo_segundos=30
)
```

### Regra operacional

Executar somente um poller por vez.

O pipeline não possui lock atômico entre executores.

## Limitações conhecidas

- a origem do arquivo não é persistida por linha na Bronze;
- `bronze_ingested_at` não existe atualmente;
- a partição atual é `data_ultima_atualizacao`;
- a solução prática utiliza Azure SDK / PyArrow / Pandas / delta-rs em pontos do fluxo;
- Bronze, Control Log e Manifest são escritos separadamente e não formam uma única transação;
- concorrência entre dois pollers simultâneos não é segura.
