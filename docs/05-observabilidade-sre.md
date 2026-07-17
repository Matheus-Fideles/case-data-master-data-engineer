# Observabilidade e SRE — Case Academia Santander Data Engineering

> Estratégia de monitoramento, lineage, runbook operacional e especificações de testes.

---

## Sumário

- [1. Os 3 Pilares da Observabilidade](#1-os-3-pilares-da-observabilidade)
- [2. Métricas — Prometheus + Grafana](#2-métricas--prometheus--grafana)
- [3. Lineage — Marquez + OpenLineage](#3-lineage--marquez--openlineage)
- [4. Logs Estruturados](#4-logs-estruturados)
- [5. Runbook Operacional](#5-runbook-operacional)
- [6. Especificação dos DAGs Airflow](#6-especificação-dos-dags-airflow)
- [7. Schemas Bronze — Especificação](#7-schemas-bronze--especificação)
- [8. Smoke Tests E2E](#8-smoke-tests-e2e)
- [9. Defesa em Banca](#9-defesa-em-banca)

---

## 1. Os 3 Pilares da Observabilidade

```
┌────────────────────────────────────────────────────────────┐
│  MÉTRICAS              LOGS                LINEAGE          │
│  ──────────            ────                ───────          │
│  Prometheus            JSON estruturado    OpenLineage      │
│  Grafana               stdout              Marquez          │
│                                                             │
│  "o quê está           "por que está       "de onde         │
│   acontecendo"          acontecendo"        veio o dado"    │
└────────────────────────────────────────────────────────────┘
```

| Pilar | Pergunta que responde | Quando consultar |
|---|---|---|
| **Métricas** | *"Algo está fora do normal agora?"* | Dashboard sempre aberto durante demo |
| **Logs** | *"Por que esse job falhou às 14h22?"* | Investigação após alerta |
| **Lineage** | *"Quem produziu este dado e quando?"* | Auditoria + impacto de schema change |

---

## 2. Métricas — Prometheus + Grafana

### Taxonomia RED (para serviços com endpoint)

Aplicado a Trino, Airflow webserver:

| Métrica | Tipo | Label obrigatório |
|---|---|---|
| `{svc}_requests_total` | Counter | `method`, `status_code` |
| `{svc}_request_errors_total` | Counter | `method`, `error_type` |
| `{svc}_request_duration_seconds` | Histogram | `method` |

### Taxonomia USE (para infraestrutura)

Aplicado a MinIO, Kafka, Postgres:

| Métrica | Tipo | Label obrigatório |
|---|---|---|
| `{resource}_utilization_ratio` | Gauge | `resource` |
| `{resource}_saturation_ratio` | Gauge | `resource` |
| `{resource}_errors_total` | Counter | `resource`, `error_type` |

### Métricas críticas por componente

**Kafka (JMX exporter → Prometheus):**
```
kafka_server_BrokerTopicMetrics_MessagesInPerSec
kafka_consumer_lag_sum{group="atendimento-consumer"}
kafka_server_ReplicaManager_UnderReplicatedPartitions
```

**Spark (porta 4040 — Spark UI):**
```
spark_job_duration_seconds{job="bronze_dengue"}
spark_streaming_batchDuration_ms
spark_streaming_processedRecords_total
```

**Airflow:**
```
airflow_dag_run_duration_seconds{dag_id="dag_bronze_dengue"}
airflow_task_failure_total{dag_id, task_id}
```

**MinIO:**
```
minio_bucket_objects_count{bucket="bronze"}
minio_s3_requests_total{api="GetObject"}
```

### Alertas configurados (`infra/grafana/provisioning/alerting/`)

| Alerta | Condição | Severidade |
|---|---|---|
| `KafkaConsumerLagHigh` | `consumer_lag > 10000` por 5 min | warning |
| `SparkJobFailed` | `airflow_task_failure_total` > 0 nos últimos 15 min | critical |
| `MinioDiskUsageHigh` | utilização > 85% | warning |
| `TrinoQueryP99High` | p99 latência > 30s | warning |

---

## 3. Lineage — Marquez + OpenLineage

Cada DAG Airflow emite eventos de lineage na task `emit_lineage` (última do grafo):

```python
# airflow/dags/_common/lineage.py
from openlineage.client import OpenLineageClient

def emit_dag_lineage(dag_id: str, inputs: list[str], outputs: list[str]) -> None:
    client = OpenLineageClient(url=os.environ["MARQUEZ_URL"])
    # emite RunEvent com datasets de entrada e saída
```

**Exemplo de rastreamento:**

```
REST API /arboviroses/dengue
  └─[dag_bronze_dengue]──→ s3a://bronze/epidemiologico/dengue/
       └─[dag_silver_notificacao]──→ s3a://silver/epidemiologico/notificacao/
            └─[dag_gold_fato_notificacao]──→ s3a://gold/epidemiologico/fatos_notificacao/
                 └─[Trino]──→ Analistas / Dashboards
```

Marquez UI: `http://localhost:5000` (profile `observability`).

---

## 4. Logs Estruturados

Todos os jobs Python/Spark emitem logs JSON estruturado para stdout:

```json
{
  "timestamp": "2024-01-15T14:30:00Z",
  "level": "INFO",
  "logger": "apps.epidemiologico.jobs.bronze_dengue",
  "dag_id": "dag_bronze_dengue",
  "run_id": "scheduled__2024-01-15T00:00:00+00:00",
  "batch_id": "B20240115",
  "records_read": 12500,
  "records_written": 12498,
  "duration_seconds": 45.2
}
```

**Acesso:** `docker compose logs -f spark-driver` ou Airflow Task Logs UI.

---

## 5. Runbook Operacional

### Pré-requisitos

| Requisito | Versão mínima | Como verificar |
|---|---|---|
| Docker Engine | 24+ | `docker --version` |
| Docker Compose | v2+ (plugin) | `docker compose version` |
| GNU Make | 3.81+ | `make --version` |
| Python | 3.11+ | `python3 --version` |
| Rancher Desktop | 1.9+ (k3s) | `kubectl version` |
| RAM livre (Compose) | ~8 GB | `vm_stat` (macOS) |
| RAM Rancher Desktop | ≥ 8 GB alocados | Rancher Desktop > Preferences > Virtual Machine |
| Disco livre | ~10 GB | `df -h .` |

Portas no host: **5432, 9000, 9001, 9083, 9092, 8080, 8085, 3000, 5000**.

### Comandos principais

```bash
# Setup inicial
cp .env.example .env        # ajustar segredos antes de tudo
make warmup                 # docker pull de todas as imagens
make seed                   # gera dados sintéticos + cache offline

# Subir serviços (por profile)
make up-core                # postgres + minio + airflow
make up-streaming           # kafka + stream-producer
make up-serving             # hive-metastore + trino
make up-observability       # prometheus + grafana + marquez
make up-all                 # todos os profiles

# Kubernetes (Spark)
make k8s-setup              # namespace spark + spark-operator (Helm)
make k8s-spark-image        # build + push imagem Spark custom

# Operação
make ps                     # status de todos os containers
make logs                   # logs de todos (Ctrl+C para sair)
make logs SVC=minio         # logs de um serviço específico
make smoke                  # testes E2E (~3 min)
make down                   # para containers, mantém volumes
make clean                  # para + remove volumes (reset completo)
```

### Troubleshooting comum

**MinIO não inicia:**
```bash
docker compose logs minio | tail -50
# Verificar se a porta 9000 está livre:
lsof -nP -iTCP:9000 -sTCP:LISTEN
```

**Hive Metastore em loop:**
```bash
docker compose logs hive-metastore | grep -i error
# Garantir que postgres está healthy antes:
docker compose ps postgres
```

**Trino não encontra tabelas Delta:**
```bash
# Verificar se HMS está healthy
docker compose exec trino trino --execute "SHOW CATALOGS;"
# Registrar tabela manualmente se necessário:
docker compose exec trino trino --execute \
  "CALL delta.system.register_table('epidemiologico', 'fatos_notificacao', 's3a://gold/epidemiologico/fatos_notificacao/');"
```

**Airflow DAG não aparece:**
```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags test dag_bronze_dengue 2024-01-15
```

**Spark job falha no k8s:**
```bash
kubectl get sparkapplications -n spark
kubectl describe sparkapplication bronze-dengue -n spark
kubectl logs -n spark -l spark-role=driver
```

---

## 6. Especificação dos DAGs Airflow

### Convenções de nomeação

| Padrão | Exemplo | Propósito |
|---|---|---|
| `dag_bronze_{fonte}` | `dag_bronze_dengue` | REST API → landing/ → Bronze Delta |
| `dag_silver_{domínio}` | `dag_silver_notificacao` | Bronze → Silver + mascaramento |
| `dag_gold_dim_{nome}` | `dag_gold_dim_municipio` | Dimensão Gold |
| `dag_gold_fato_{nome}` | `dag_gold_fato_notificacao` | Fato Gold |
| `dag_maint_{ação}` | `dag_maint_optimize_delta` | Manutenção Delta |
| `dag_quality_{domínio}` | `dag_quality_gates_silver` | Validações Great Expectations |

### Default args (todas as DAGs)

```python
default_args = dict(
    owner="data-eng",
    retries=2,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
    sla=timedelta(hours=2),
)
```

### Grafo padrão de DAG Bronze

```
pre_check → extract [Python] → validate_raw [Python] → ingest [SparkK8s] → quality_gate → emit_lineage
```

| Task | Operador | Responsabilidade |
|---|---|---|
| `pre_check` | PythonOperator | Verifica cache offline, conectividade |
| `extract` | PythonOperator | Chama `apps/{domínio}/jobs/extract_*.py` → `s3a://landing/{fonte}/` |
| `validate_raw` | PythonOperator | Assert `row_count > 0`, schema mínimo presente |
| `ingest` | SparkKubernetesOperator | Submete SparkApplication CRD; lê landing → Bronze Delta |
| `quality_gate` | PythonOperator | Suite Great Expectations |
| `emit_lineage` | PythonOperator | OpenLineage emit via Marquez |

### DAGs existentes

| DAG | Schedule | Domínio |
|---|---|---|
| `dag_bronze_dengue` | `@monthly` | epidemiologico |
| `dag_bronze_zika` | `@monthly` | epidemiologico |
| `dag_bronze_chikungunya` | `@monthly` | epidemiologico |
| `dag_bronze_sim_obitos` | `@monthly` | hospitalar |
| `dag_bronze_cnes` | `@monthly` | hospitalar |
| `dag_bronze_vacinacao` | `@monthly` | vacinal |
| `dag_bronze_municipios` | `@yearly` | geografico |
| `dag_bronze_oltp_paciente` | `@daily` | oltp |
| `dag_silver_notificacao` | `@monthly` | epidemiologico |
| `dag_silver_paciente` | `@daily` | oltp |
| `dag_gold_dim_paciente` | `@daily` | oltp |
| `dag_gold_dim_estabelecimento` | `@monthly` | hospitalar |
| `dag_gold_fato_notificacao` | `@monthly` | epidemiologico |
| `dag_maint_optimize_delta` | `@weekly` | manutenção |
| `dag_quality_gates_gold` | `@daily` | qualidade |

---

## 7. Schemas Bronze — Especificação

### Princípios da camada Bronze

1. **Bronze = raw + rastreabilidade.** Mínima transformação; casts apenas onde inevitável.
2. **Schema explícito.** `StructType` declarado em `apps/shared/schemas.py`; `mergeSchema=true` permite colunas novas.
3. **Metadados padrão em todas as tabelas:**
   - `_ingestion_ts` (TimestampType) — quando o job escreveu a linha
   - `_batch_id` (StringType) — `run_id` do Airflow
   - `ano_mes` (StringType) — coluna de partição gerada

### Tabelas Bronze por domínio

| Tabela | Path | Partição | Chave idempotência |
|---|---|---|---|
| `dengue` | `s3a://bronze/epidemiologico/dengue/` | `(nu_ano, sg_uf_not)` | hash composto |
| `zika` | `s3a://bronze/epidemiologico/zika/` | `(nu_ano, sg_uf_not)` | hash composto |
| `chikungunya` | `s3a://bronze/epidemiologico/chikungunya/` | `(nu_ano, sg_uf_not)` | hash composto |
| `sim_obitos` | `s3a://bronze/hospitalar/sim_obitos/` | `(ano_obito, sg_uf_ocor)` | `nk_dec_obito` |
| `cnes_estabelecimentos` | `s3a://bronze/hospitalar/cnes_estabelecimentos/` | `(co_uf, snapshot_date)` | `co_cnes` |
| `vacinacao_pni` | `s3a://bronze/vacinal/vacinacao_pni/` | `(ano_mes, sg_uf)` | `nk_codigo_documento` |
| `municipios` | `s3a://bronze/geografico/municipios/` | nenhuma | `co_municipio` |
| `paciente` | `s3a://bronze/oltp/paciente/` | `snapshot_date` | `id_paciente` (UUID) |
| `atendimentos_stream` | `s3a://bronze/streaming/atendimentos_stream/` | `ano_mes` | `id_atendimento` |

---

## 8. Smoke Tests E2E

### Filosofia

Smoke tests respondem a uma pergunta: *"O fluxo end-to-end está funcionando?"*

- ✅ Cobre o caminho feliz com dados de fixture
- ✅ Verifica integração entre serviços (MinIO, Postgres, Kafka, Spark)
- ✅ Valida invariantes críticas (PII não vaza, idempotência, SCD2 íntegro)
- ❌ Não cobre edge cases (responsabilidade dos unit tests)
- ❌ Não cobre performance (responsabilidade de load tests)

### Targets de tempo

| Ambiente | Target |
|---|---|
| Local (`make smoke`) | < 3 min |
| CI GitHub Actions | < 5 min |
| Pre-demo (`make smoke-full`) | < 10 min |

### Suíte de testes

| Arquivo | O que valida |
|---|---|
| `test_01_infra_health.py` | Todos os serviços respondem (MinIO, Postgres, Kafka, Trino, HMS) |
| `test_02_bronze_ingestion.py` | Bronze Delta gravado com schema correto + metadados padrão |
| `test_03_silver_masking.py` | PII não existe em Silver; `id_paciente_hash` presente |
| `test_04_gold_scd2.py` | SCD2 íntegro: exatamente 1 `is_current=TRUE` por hash |
| `test_05_streaming_e2e.py` | Kafka → Spark → Bronze → Gold; DLQ recebe eventos inválidos |
| `test_06_idempotency.py` | Reprocessar mesma carga = mesmo resultado (contagem estável) |
| `test_07_quality_gates.py` | Great Expectations: nulls, ranges, unicidade |

### Execução

```bash
# Requer compose rodando (make up-all) e k8s setup
make smoke              # roda test_01 a test_05
make smoke-full         # roda todos os 7

# Individual
pytest tests/smoke/test_03_silver_masking.py -v -m smoke
```

---

## 9. Defesa em Banca

**"Como você monitora que um pipeline Spark falhou?"**
> "Três camadas: (1) Airflow Task Logs mostram o stacktrace do job; (2) Prometheus alerta via `airflow_task_failure_total > 0` em 15 min e o alerta `SparkJobFailed` dispara no Grafana; (3) Marquez registra o RunEvent como FAILED com o estado do DAG. Para pipelines streaming, o consumer lag no Kafka alerta se o Spark parou de consumir."

**"O que é lineage e para que serve neste case?"**
> "Lineage rastreia a origem e o destino de cada dado — 'este fato_notificacao veio de qual REST API, passou por qual job Spark, em qual execução Airflow'. Implementamos via OpenLineage: cada DAG emite eventos para o Marquez ao final. Isso responde ao requisito 4 do enunciado ('rastrear fluxo de dados') e é crucial para análise de impacto — se o schema da API de dengue mudar, consigo ver quais tabelas Silver e Gold são afetadas."

**"Como você garante que a reexecução de um DAG não duplica dados?"**
> "Idempotência por MERGE (ADR-002). Todo job de ingestão usa `MERGE` por chave natural. Se o mesmo batch for processado duas vezes, os registros existentes são atualizados (ou ignorados para `whenNotMatchedInsert`) em vez de duplicados. `BRONZE_PATH` + `batch_id` são parâmetros fixos por execução Airflow."
