# Guia de Diagramas — Plataforma de Vigilância Epidemiológica

Documentação de referência para desenhar os três diagramas da arquitetura no draw.io.

---

## Diagrama 1 — Arquitetura de Solução (visão geral)

**Título:** Plataforma de Vigilância Epidemiológica — Arquitetura de Solução  
**Layout:** esquerda → direita (LR), 7 grupos (swimlanes) lado a lado

### Grupos — da esquerda para direita

**1. FONTES DE DADOS** (cor: laranja)
- REST API — Ministério da Saúde (SINAN, SIM, CNES, PNI, IBGE)
- Postgres OLTP — Cadastro de Pacientes com PII (Faker sintético)
- Stream Producer — Docker, Faker → JSON, 10 eventos/seg

**2. ORQUESTRAÇÃO** (cor: azul)
- Apache Airflow 2.9 — `:8080`, Scheduler + CeleryExecutor, aciona jobs batch e Spark

**3. SPEED LAYER** (cor: índigo)
- Apache Kafka KRaft — `:9092`, tópico `notif.raw` (3 partições), `notif.dlq` (1 partição)

**4. PROCESSAMENTO — k3s** (cor: azul-acinzentado)
- Rancher Desktop k3s — namespace `spark`, SA + RBAC, secret `pii-secret`
- Spark Operator (Helm 1.1.27) → SparkApplication Batch + SparkApplication Streaming

**5. DATA LAKE — MinIO / Delta Lake** (cor: âmbar, com 3 sub-grupos internos)
- MinIO — `:9000` S3 API, `:9001` Console
  - **Bronze** `s3a://bronze/` — raw, MERGE NK, ACID
  - **Silver** `s3a://silver/` — limpo, PII masking SHA-256+salt
  - **Gold** `s3a://gold/` — star schema + SCD2 Postgres

**6. SERVING LAYER** (cor: verde)
- Hive Metastore — `thrift:9083`, catálogo Delta
- Trino 448 — `:8085` UI, `:8086` JDBC, `catalog: delta`
- Postgres RBAC — roles: reader, analyst, admin

**7. OBSERVABILIDADE** (cor: cinza)
- Prometheus — `:9090`, retention 15d
- Grafana — `:3000`, dashboards + SLOs
- Marquez (OpenLineage) — `:5000` API, `:5001` UI → **caixa laranja**, não usar ícone Jaeger

### Conexões

```
REST API        ──REST mensal──────►  Airflow
Postgres OLTP   ──snapshot diário──►  Airflow
Airflow         ──SparkK8sOp───────►  k3s → Spark Batch

Stream Producer ──JSON─────►  Kafka
Kafka           ──consume──►  Spark Streaming

Spark (Batch + Stream)  ──MERGE Delta──►  MinIO
MinIO   ──►  Bronze  ──transform + masking──►  Silver  ──star schema──►  Gold
Silver  ──SCD2──►  Gold Postgres

Gold          ──cataloga──►  Hive Metastore
Gold Postgres ──expõe────►  Hive Metastore
Hive Metastore ──────────►  Trino ──────────►  Postgres RBAC

Airflow    ─ ─ OpenLineage ─ ─►  Marquez     (tracejado, laranja)
Spark      ─ ─ /metrics ─ ─ ─►  Prometheus  (tracejado, cinza)
Prometheus ──────────────────►  Grafana
```

---

## Diagrama 2 — Fluxo de Dados Medallion

**Título:** Fluxo de Dados — Arquitetura Medallion (Bronze → Silver → Gold)  
**Layout:** esquerda → direita (LR), 8 colunas de grupos

### Grupos — da esquerda para direita

**1. FONTES DE DADOS** (cor: laranja)
- REST API MS — SINAN, SIM, CNES, PNI, IBGE
- Postgres OLTP — ⚠ PII: CPF, nome, data de nascimento
- Kafka KRaft — `notif.raw`, 10 evt/seg
- Cache Offline — `data/raw/`, `OFFLINE_MODE=1` → **caixa tracejada vermelho claro**

**2. INGESTÃO — Spark** (cor: azul)
- Spark Ingestão Batch — `apps/*/jobs/ingest_*.py`, MERGE NK upsert
- Spark Structured Streaming — `apps/streaming/jobs/stream_consumer.py`

**3. BRONZE** `s3a://bronze/` (cor: amarelo âmbar)
- `epidemiologico/` — dengue, zika, chikungunya — Part: nu_ano, uf
- `hospitalar/` — sim_obitos, cnes — Part: ano, uf
- `vacinal+geo/` — vacinacao_pni, municipios_ibge
- `oltp/paciente/` — ⚠ **PII PRESENTE** → **caixa vermelha**
- `streaming/` — atendimentos_rt, watermark 1h, Delta append
- Nota interna: Schema Evolution · Time Travel · MERGE NK upsert · sem PII masking

**4. TRANSFORMAÇÃO + PII MASKING** (cor: azul escuro)
- Spark Transformação — `apps/*/jobs/transform_*.py` + Great Expectations
- PII Masking (ADR-004) — `apps/shared/masking.py`, CPF → SHA-256+salt, `PII_SALT` em k8s secret

**5. SILVER** `s3a://silver/` (cor: verde-azulado)
- `arboviroses/` — decode faixa etária, datas ISO, CID-10 join
- `hospitalar_norm/` — CID-10 decode, geocode IBGE
- `vacinacao_norm/` — faixa etária std, CNES enrich, cobertura % município
- `paciente_anon/` — ✓ CPF→SHA256+salt, id_hash sem PII → **caixa verde especial**
- `atend_norm/` — validação schema, erros → `notif.dlq`, Delta append
- Nota interna: sem PII raw · DLQ tratado · schema validado · ready for analytics

**6. STAR SCHEMA + SCD2** (cor: azul escuro)
- Spark Star Schema — `apps/*/jobs/gold_*.py`, MERGE fato + dim
- SCD2 Postgres — dim_paciente, dim_estabelecimento, dt_inicio, is_current

**7. GOLD** `s3a://gold/` + Postgres SCD2 (cor: dourado)
- **Delta Lake:**
  - `fato_notificacao` — id_notif, id_pac_hash, id_mun, id_vac, data
  - `fato_atendimento` — streaming → append, id_atend, cid10
  - `fato_vacinacao` — id_vac, id_mun, qt_doses, cobertura
  - `dim_municipio` — ibge_cod, nome, uf, regiao, lat/lon
- **Postgres SCD2:**
  - `dim_paciente` — id_hash, faixa_etaria, dt_inicio, is_current
  - `dim_estabelecimento` — cnes_cod, nome, dt_inicio, is_current

**8. SERVING** (cor: verde escuro)
- Hive Metastore — `thrift:9083`, catálogo Delta
- Trino 448 — `:8085` / `:8086`, `SELECT gold.*`
- Postgres RBAC — roles reader/analyst/admin, GRANT por schema

### Conexões

```
REST API      ──REST mensal──►  Spark Ingestão
Postgres OLTP ──JDBC diário──►  Spark Ingestão
Kafka         ──consume─────►  Spark Streaming

Spark Ingestão  ──MERGE NK──►  Bronze (epi, hosp, vac, oltp, stream)

Bronze (epi, hosp, vac, stream)  ────────────►  Spark Transformação
Bronze (oltp/paciente ⚠ PII)     ──masking───►  Módulo PII Masking

Spark Transformação  ──►  Silver (arboviroses, hosp_norm, vac_norm, atend_norm)
Módulo PII Masking   ──id_hash ✓──►  Silver (paciente_anon)

Silver (todos)        ──────►  Spark Star Schema
Silver (paciente_anon) ──SCD2──►  SCD2 Postgres

Spark Star Schema  ──►  Gold Delta (fatos + dim_municipio)
SCD2 Postgres      ──►  Gold Postgres (dim_paciente, dim_estabelecimento)

Gold Delta     ──cataloga──►  Hive Metastore
Gold Postgres  ──expõe────►  Trino
Hive Metastore ───────────►  Trino  ──RBAC──►  Postgres RBAC
```

---

## Diagrama 3 — Deployment Local

**Título:** Deployment Local — Docker Compose Profiles + Rancher Desktop k3s  
**Layout:** dois blocos principais — Docker Compose (esquerda, 4 grupos empilhados verticalmente) + k3s (direita, um bloco alto)

### Bloco esquerdo — Docker Compose (4 perfis empilhados)

**PERFIL CORE** `make demo` (cor: azul)
- Redis 7 — `:6379`, Celery broker, task queue
- Apache Airflow 2.9 — `:8080`, Scheduler + Worker, CeleryExecutor
- PostgreSQL 15 — `:5432`, schemas: oltp, gold, airflow
- MinIO — `:9000` S3 API, `:9001` Console, buckets: bronze, silver, gold
- Nota: volumes `postgres_data`, `minio_data`, `redis_data`, `airflow_logs`

**PERFIL STREAMING** `make up-streaming` (cor: laranja)
- Stream Producer — `apps/streaming/producer/`, Faker → JSON, 10 evt/seg
- Apache Kafka KRaft — `:9092`, `notif.raw` (3 part.), `notif.dlq` (1 part.)
- Kafka UI — `:8082`, topics + consumer groups + offsets
- Depends on: CORE

**PERFIL SERVING** `make up-serving` (cor: verde)
- Hive Metastore — `thrift:9083`, conecta Postgres (schema: metastore) + MinIO
- Trino 448 — `:8085` UI, `:8086` JDBC, `catalog: delta → HMS`
- Depends on: CORE

**PERFIL OBSERVABILIDADE** `make up-observability` (cor: roxo)
- Prometheus — `:9090`, retention 15d, scrape Airflow + Spark Driver
- Grafana — `:3000`, datasource: Prometheus
- Marquez (OpenLineage) — `:5000` API, `:5001` UI → **caixa laranja** (não usar ícone Jaeger)

### Bloco direito — Rancher Desktop k3s `make k8s-setup` (cor: azul escuro, bloco alto)

**Infraestrutura:**
- k3s single-node — namespace `spark`, SA: `spark-sa` (RBAC), secret: `pii-secret` (contém PII_SALT)
- Spark Operator — Helm chart 1.1.27, SparkApplication CRDs, `kubectl apply -f k8s/`

**SparkApplication: spark-batch** `k8s/spark-batch.yaml` (cor: amarelo)
- Driver Pod — 2 CPU, 2 GB RAM, `image: spark:3.5`
- Executor Pod 1 — 4 CPU, 4 GB RAM, 2 cores/executor
- Executor Pod 2 — 4 CPU, 4 GB RAM, 2 cores/executor
- Great Expectations — suites: `bronze_suite`, `silver_suite` → resultado para Airflow
- Jobs: `ingest_sinan`, `ingest_sim`, `ingest_cnes`, `ingest_pni`, `ingest_ibge`, `ingest_oltp_snapshot`, `transform_*`, `gold_star_schema`
- Trigger: Airflow `SparkKubernetesOperator` + `SparkKubernetesJobSensor`

**SparkApplication: spark-streaming** `k8s/spark-streaming.yaml` (cor: verde-azulado)
- Driver Pod — 1 CPU, 1 GB RAM
- Executor Pod — 2 CPU, 2 GB RAM, micro-batch 30s
- Checkpoint — `s3a://bronze/streaming/_checkpoints/`, offset tracking
- DLQ Handler — erros → `notif.dlq`, schema inválido, retry 3x
- Consume: Kafka `notif.raw`, watermark 1h
- Always-on: deploy direto via kubectl, **não** controlado pelo Airflow

**Mapeamento 1:1 AWS** (cor: cinza, tabela interna no bloco k3s)

| Componente Local | Equivalente AWS |
|---|---|
| MinIO | S3 + Delta Lake |
| Hive Metastore | AWS Glue Catalog |
| Kafka KRaft | MSK Serverless |
| Spark Operator (k3s) | EMR Serverless |
| Airflow CeleryExecutor | MWAA |
| PostgreSQL 15 | RDS Aurora PostgreSQL |
| Trino 448 | Athena / Trino no EKS |
| Prometheus + Grafana | AMP + Amazon Managed Grafana |
| Marquez (OpenLineage) | AWS DataZone |
| Postgres RBAC | IAM + Lake Formation |

### Conexões

```
Redis ─ ─ task queue ─ ─►  Airflow

Airflow  ──SparkKubernetesOperator──►  k3s → Spark Operator
Spark Operator  ──SparkApplication──►  Batch Driver
Spark Operator  ──SparkApplication──►  Streaming Driver

Stream Producer  ──JSON eventos──►  Kafka
Kafka  ──consume notif.raw──►  Streaming Driver

Batch Driver     ─ ─ s3a:// ─ ─►  MinIO   (tracejado)
Streaming Driver ─ ─ append ─ ─►  MinIO   (tracejado)

Airflow  ─ ─ metadata ─ ─►  Postgres   (tracejado)

MinIO          ──Delta files──►  Hive Metastore
Hive Metastore ──catálogo────►  Trino

Airflow    ─ ─ /metrics ─ ─►  Prometheus   (tracejado, cinza)
Prometheus ──────────────────►  Grafana
Airflow    ─ ─ OpenLineage ─►  Marquez      (tracejado, laranja)
```

---

## Dicas visuais para o draw.io

| Elemento | Configuração |
|---|---|
| Arestas principais | `edgeStyle=orthogonalEdgeStyle; rounded=0` |
| Arestas de observabilidade | `dashed=1; strokeColor=#888888` |
| Arestas OpenLineage (Marquez) | `dashed=1; strokeColor=#E65100` |
| Marquez | `fillColor=#FFE0B2; strokeColor=#E65100` |
| Itens com PII | `fillColor=#FFCCBC; strokeColor=#BF360C` |
| Cache Offline | `fillColor=#FFCCBC; strokeColor=#BF360C; dashed=1` |
| paciente_anon (Silver) | `fillColor=#A5D6A7; strokeColor=#1B5E20` |
| mxGraphModel | `shadow="0"` (obrigatório para exportação via CLI sem erro) |
