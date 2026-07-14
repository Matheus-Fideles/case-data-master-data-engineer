# ADR 0008 — Separação de Camadas de Ingestão: Python (extração) + PySpark (transformação)

- **Status:** Aceito
- **Data:** 2026-07-13
- **Contexto:** definir qual ferramenta executa cada etapa do pipeline de dados

## Contexto

O pipeline de dados passa por etapas com características técnicas distintas. A decisão é sobre qual ferramenta executa cada etapa e por quê — evitando tanto subutilização (usar Spark para chamar uma API REST) quanto subutilização inversa (usar Python puro para processar Delta Lake distribuído).

## Etapas e decisões

### Etapa 1 — Extração: API REST → MinIO `landing/`

**Ferramenta escolhida: Python puro via `PythonOperator` no Airflow**

| Critério | Python requests | PySpark |
|---|---|---|
| Chamada HTTP paginada | ✅ direto (`requests.get`, loop offset) | ❌ overhead de SparkContext para I/O serial |
| Retry e rate limiting | ✅ `tenacity`, `backoff` | Complexo (não é uso nativo) |
| Modo offline (cache) | ✅ simples (`if cache_exists: load`) | Possível mas verbose |
| Tempo de startup | < 1s | 10–30s (JVM + SparkContext) |
| Output | JSON/NDJSON em `s3://landing/` | N/A |

**Justificativa:** extração é I/O serial paginado — não há dado suficiente em memória para justificar Spark. Usar Spark aqui seria overhead sem benefício e tornaria o DAG mais lento sem motivo.

### Etapa 2 — Bronze: `landing/` → `s3://bronze/` (Delta)

**Ferramenta escolhida: PySpark via `SparkKubernetesOperator`**

| Critério | Python (pandas/polars) | **PySpark** |
|---|---|---|
| Escrita Delta ACID | Via `deltalake` lib (limitada) | ✅ nativo, suporte completo a MERGE |
| Schema enforcement | Manual | ✅ `StructType` + `mergeSchema` |
| Particionamento | Manual (`os.makedirs`) | ✅ `partitionBy` no writer |
| Escalabilidade | Single-node | ✅ distribuído |
| Idempotência via MERGE | Reimplementar do zero | ✅ `DeltaTable.merge()` |

### Etapa 3 — Silver: `bronze/` → `s3://silver/` (Delta + masking PII)

**Ferramenta escolhida: PySpark**

UDFs de mascaramento (`sha2`, `substring`, `lit(null)`) são operações nativas do Spark SQL — rodam distribuídas sem código adicional. A mesma UDF funciona para batch e streaming (reutilização).

### Etapa 4 — Gold: `silver/` → `s3://gold/` + `postgres.gold_dw`

**Ferramenta escolhida: PySpark**

SCD Tipo 2 requer lógica de `MERGE` com condições complexas por `nk_*`. Delta MERGE é a implementação mais limpa e confiável. Joins dimensionais (lookup de SK) em datasets de centenas de mil linhas se beneficiam de distribuição.

### Etapa 5 — Streaming consumer: Kafka → Bronze stream → Gold stream

**Ferramenta escolhida: PySpark Structured Streaming**

Mesma API de batch — `readStream` em vez de `read`. Reutiliza UDFs de mascaramento Silver. Watermark e `foreachBatch` com MERGE Delta garantem exactly-once.

### Etapa 6 — Streaming producer: Faker → Kafka

**Ferramenta escolhida: Python puro (container `stream-producer`)**

Geração de eventos Faker e publicação em Kafka não precisa de Spark. Um loop Python com `confluent_kafka.Producer` é mais simples, mais rápido de iniciar e mais fácil de debugar.

## Fluxo completo

```
REST API (7 endpoints)
    │
    └──[PythonOperator · Airflow]──────────────────→ s3://landing/  (JSON/NDJSON)
              │
              └──[SparkKubernetesOperator · k8s]──→ s3://bronze/   (Delta)
                          │
                          └──[SparkKubernetesOperator · k8s]──→ s3://silver/  (Delta + masking)
                                        │
                                        └──[SparkKubernetesOperator · k8s]──→ s3://gold/ + postgres.gold_dw

Faker (Python container)
    │
    └──[Producer]──→ Kafka topic: notificacoes.raw
                          │
                          └──[Spark Structured Streaming · k8s]──→ s3://bronze/stream/ ──→ s3://gold/
```

## Consequências

- DAGs têm dois tipos de task: `PythonOperator` (extração) + `SparkKubernetesOperator` (transformação)
- Extratores vivem em `pipelines/extraction/` — Python puro, sem dependência Spark
- Jobs Spark vivem em `pipelines/batch/` e `pipelines/streaming/` — PySpark puro
- O pool `extraction_pool` (8 slots) limita concorrência de chamadas API
- O pool `spark_pool` (4 slots) limita jobs Spark concorrentes no k8s

## Defesa em banca

*"Separar extração de transformação segue o princípio de responsabilidade única. A extração é I/O serial — chamar uma API, paginar, salvar em landing. Não há dado suficiente em memória para justificar a JVM do Spark. Já a transformação Bronze→Gold é computação distribuída sobre dados estruturados — exatamente o que Spark foi projetado para fazer. Misturar os dois em um único job Spark tornaria a extração mais lenta e o job mais difícil de testar."*
