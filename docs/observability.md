# Estratégia de Observabilidade

> **Audiência:** banca · candidato · operadores
> **Cobertura:** Requisito 4 do enunciado — *"Estratégia de monitoramento e observabilidade da solução, garantindo a capacidade de rastrear o fluxo de dados, detectar falhas e identificar possíveis gargalos de desempenho entre as ferramentas que compõem o(s) pipeline(s) de ingestão."*

## Os 3 pilares da observabilidade

```
┌────────────────────────────────────────────────────────────┐
│  MÉTRICAS              LOGS                LINEAGE          │
│  ──────────            ────                ───────          │
│  Prometheus            JSON estruturado    OpenLineage      │
│  Grafana                "to stdout"         Marquez          │
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

## Métricas — taxonomia adotada

### RED (para serviços com endpoint)

Aplicado a Trino, Airflow webserver, Metabase:
- **R**ate — requisições por segundo (`http_requests_total`)
- **E**rrors — taxa de erro 5xx (`http_requests_total{code=~"5.."}`)
- **D**uration — latência percentis (`http_request_duration_seconds_bucket`)

### USE (para recursos)

Aplicado a Postgres, Kafka, Spark workers, MinIO:
- **U**tilization — % uso (CPU, memória, disco)
- **S**aturation — fila / espera (queries esperando, lag de consumer)
- **E**rrors — falhas no recurso (timeouts, oom kills)

### DataOps (custom — para pipelines)

Métricas específicas de engenharia de dados que **não cabem** em RED nem USE:

| Métrica | Tipo | Labels | O que indica |
|---|---|---|---|
| `pipeline_runs_total` | counter | `dag, status` | Volume de execuções |
| `pipeline_duration_seconds` | histogram | `dag, task` | Performance |
| `pipeline_records_written_total` | counter | `dag, table, mode` | Throughput |
| `pipeline_records_rejected_total` | counter | `dag, table, reason` | Qualidade |
| `data_freshness_seconds` | gauge | `layer, table` | Latência de dado |
| `schema_drift_total` | counter | `table, change_type` | Mudança upstream |
| `quality_check_failed_total` | counter | `dag, expectation` | Validação Great Expectations |
| `pii_in_silver_check_failed` | gauge | — | **Sentinela crítica**: deve ser sempre 0 |
| `masking_records_total` | counter | `technique` | Volume mascarado por técnica |
| `masking_failures_total` | counter | `dag, reason` | Falha de mascaramento |
| `lgpd_erasure_requests_total` | counter | `status` | Pedidos de esquecimento |
| `kafka_consumer_lag` | gauge | `topic, partition` | Atraso do streaming |
| `stream_dlq_count` | gauge | `topic, reason` | Mensagens em DLQ |
| `stream_processing_lag_seconds` | gauge | `query` | Atraso processamento |
| `delta_files_count` | gauge | `table` | Indicador de small files (precisa OPTIMIZE) |

### Métricas por componente

#### Postgres (source e DW)

Exporter: `prometheus-postgres-exporter`

| Métrica padrão | Por que importa |
|---|---|
| `pg_up` | Disponibilidade |
| `pg_database_size_bytes` | Crescimento do DW |
| `pg_stat_database_xact_commit` | Throughput de transações |
| `pg_stat_replication_lag_bytes` | Em prod, lag de réplica |
| `pg_locks_count` | Locks segurando carga |

#### MinIO

Métricas Prometheus nativas em `:9000/minio/v2/metrics/cluster`:

| Métrica | Importância |
|---|---|
| `minio_bucket_usage_total_bytes` | Crescimento de Bronze/Silver/Gold |
| `minio_bucket_usage_object_total` | Contagem de objetos (small files alert) |
| `minio_s3_requests_inflight_total` | Concorrência |
| `minio_s3_requests_errors_total` | Erros |

#### Kafka

Exporter: `kafka-exporter` (Confluent) ou JMX exporter

| Métrica | Importância |
|---|---|
| `kafka_topic_partition_current_offset` | Cabeça do tópico |
| `kafka_consumer_lag` | **Métrica chave do streaming** |
| `kafka_topic_partitions` | Sanity (não cresceu sem aviso) |
| `kafka_brokers` | Quantidade de brokers vivos |

#### Spark

Métricas via callback Prometheus em `spark.metrics.conf`:

| Métrica | Importância |
|---|---|
| `spark_driver_memory_used_bytes` | Memória do driver |
| `spark_executor_memory_used_bytes` | Memória executor |
| `spark_executor_failed_tasks_total` | Falhas de tasks |
| `spark_streaming_query_lastBatchExecutionTime` | Saúde do streaming |
| `spark_streaming_query_inputRowsPerSecond` | Throughput streaming |

#### Airflow

`StatsD` exporter convertido para Prometheus

| Métrica | Importância |
|---|---|
| `airflow_dag_run_duration` | Duração média por DAG |
| `airflow_dag_processing_total_parse_time` | Saúde do scheduler |
| `airflow_executor_open_slots` | Capacidade |
| `airflow_ti_failures` | Falhas de tasks |

## Dashboards Grafana planejados

> Dashboards JSON ficam em `infra/grafana/dashboards/`. **Provisioning** automático via `infra/grafana/provisioning/dashboards/`.

### Dashboard 1 — Visão Executiva

Audiência: banca + stakeholder
Layout em uma tela; sem scroll.

| Painel | Tipo | Período |
|---|---|---|
| Pipelines em execução agora | Stat (count) | now |
| Pipelines com falha últimas 24h | Stat (count, threshold red ≥1) | 24h |
| Linhas processadas hoje | Stat (sum) | today |
| Latência da camada Gold | Stat (P95 of `data_freshness_seconds`) | 1h |
| Eventos streaming/seg | Time-series | 1h |
| Saúde dos serviços | Status row (up/down por serviço) | now |

### Dashboard 2 — Pipelines Batch

| Painel | Métrica |
|---|---|
| Duração de cada DAG (últimos 30 dias) | `pipeline_duration_seconds` heatmap |
| Taxa de sucesso por DAG | `pipeline_runs_total{status} / total` |
| Volume escrito por camada | `pipeline_records_written_total{layer}` stacked area |
| Drift de schema | `schema_drift_total{table}` table |
| Top 5 DAGs mais lentas | `topk(5, avg by (dag) (pipeline_duration_seconds))` |

### Dashboard 3 — Streaming

| Painel | Métrica |
|---|---|
| Eventos/seg producer | `rate(producer_records_sent_total[1m])` |
| Lag do consumer | `kafka_consumer_lag` time-series por partição |
| Latência ponta-a-ponta | `stream_processing_lag_seconds` |
| Eventos em DLQ por motivo | `stream_dlq_count{reason}` stacked |
| Janela watermark atual | `stream_watermark_seconds_behind` |
| Taxa de erro do Spark Streaming | falhas / batches |

### Dashboard 4 — Qualidade de Dados

| Painel | Métrica |
|---|---|
| Quality checks falhando | `quality_check_failed_total{expectation}` |
| Linhas rejeitadas Bronze→Silver | `pipeline_records_rejected_total` |
| Cobertura de testes (% de tabelas com suite GE) | gauge |
| **Sentinela PII** | `pii_in_silver_check_failed` — vermelho se ≠ 0 |
| Drift de count vs ontem | comparação |

### Dashboard 5 — Lineage e Auditoria

Acessa Marquez UI iframe para visualização do grafo. Métricas complementares:

| Painel | Métrica |
|---|---|
| Pipelines mascarando hoje | `masking_records_total` |
| Falhas de mascaramento | `masking_failures_total` |
| Pedidos LGPD | `lgpd_erasure_requests_total{status}` |
| Auditoria de acesso ao Bronze (S2+) | (logs estruturados convertidos) |

## Alertas

### Severidade

| Severidade | Latência de resposta | Canal |
|---|---|---|
| **🔴 Crítico** | minutos | PagerDuty + Slack #data-eng-pager |
| **🟠 Alto** | 1h horário comercial | Slack #data-eng-alerts |
| **🟡 Médio** | 1 dia útil | Slack #data-eng-alerts |
| **🔵 Info** | sem ação imediata | log apenas |

### Regras de alerta (PromQL exemplos)

> Em produção, ficariam em `infra/prometheus/alerts.yml` e seriam carregadas no Prometheus.

| Alerta | Severidade | Regra (PromQL) | Ação |
|---|---|---|---|
| **PII em Silver** | 🔴 | `pii_in_silver_check_failed > 0` | Pausar pipelines downstream + page |
| Falha de mascaramento >0.1% | 🔴 | `rate(masking_failures_total[5m]) / rate(masking_records_total[5m]) > 0.001` | Investigar bug |
| DAG falhou 2 vezes seguidas | 🟠 | `airflow_dag_run_consecutive_failures >= 2` | Investigar |
| Lag Kafka > 1000 | 🟠 | `kafka_consumer_lag > 1000` por 5min | Verificar Spark Streaming |
| Disco MinIO > 80% | 🟠 | `(minio_disk_used / minio_disk_total) > 0.8` | Vacuum / cleanup |
| Data freshness > 1 dia | 🟡 | `data_freshness_seconds > 86400` | Investigar fonte |
| DLQ crescendo | 🟡 | `increase(stream_dlq_count[1h]) > 100` | Análise das mensagens |
| Schema drift detectado | 🟡 | `schema_drift_total > 0` em janela | PR review |
| Spark executor OOM | 🟠 | `spark_executor_oom_total > 0` | Tunar memória |
| Postgres conexões > 80% pool | 🟠 | `pg_stat_activity_count / pg_settings_max_connections > 0.8` | Limitar pool |

### Política de roteamento

- Alertas críticos vão para **PagerDuty** com escalonamento de 15min se não houver acknowledge
- Alertas altos/médios para **Slack**
- Silenciamento programado durante janela de manutenção (`make maintenance-window`)

## Logs estruturados

### Formato JSON obrigatório

Cada linha de log emitida por pipelines tem o mesmo envelope:

```json
{
  "timestamp": "2026-05-21T14:30:25.123Z",
  "level": "INFO",
  "logger": "pipelines.batch.bronze_ingest_datasus",
  "pipeline_name": "dag_bronze_datasus_sih",
  "run_id": "scheduled__2024-01-01T06:00:00+00:00",
  "task_id": "ingest",
  "dataset": "bronze.sih_sus",
  "partition": "uf=SP/ano_mes=2024-01",
  "record_count": 250431,
  "duration_seconds": 142.7,
  "message": "Ingestion completed"
}
```

### Regras

- **Nunca** logar PII em claro (mesmo em DEBUG) — bloqueado por code review e por linter custom (regex `cpf|email|telefone` em strings)
- **Sempre** incluir `run_id` para correlação cross-DAG
- **Estruturado**, não texto livre — facilita ingestão para Loki (S4 evolução) ou Datadog (prod)

### Onde os logs vão (S1–S4)

| Componente | Saída de log |
|---|---|
| Airflow tasks | `airflow/logs/<dag_id>/<task_id>/<run_id>/<try>.log` (JSON) |
| Spark | `stderr` do container — `docker logs spark-worker` |
| Pipelines Python | `stdout` capturado pelo Airflow |
| Stream consumer | `docker logs stream-consumer` |
| Postgres | `pg_log` do volume |
| Kafka | logs no volume Kafka |

### Onde **NÃO** vão (S1–S4)

- Loki centralizado: **fora de escopo** (cortado em [`scope.md`](./architecture/scope.md))
- Splunk/Datadog: produção; aqui só evolução

## Lineage (OpenLineage + Marquez)

### O que é capturado

- **Job** — DAG ou Spark application
- **Run** — execução específica de um job
- **Dataset** — tabela ou path lake (entrada e saída do job)
- **Schema** — colunas e tipos do dataset no momento

### Captura

| Origem | Como |
|---|---|
| Airflow tasks | Provider `apache-airflow-providers-openlineage` automaticamente emite eventos |
| Spark jobs | `OpenLineageSparkListener` registrado em `spark.extraListeners` |
| dbt models (S2 opcional) | `dbt-openlineage` plugin |

### Backend

Marquez (`marquezproject/marquez`) — UI em `:3002`, API em `:5000`. Backend Postgres dedicado.

### Casos de uso para a banca

| Pergunta da banca | Resposta via Marquez |
|---|---|
| "De onde vieram os dados em `gold.fato_internacao`?" | Mostrar grafo: SIH-SUS CSV → Bronze → Silver → Gold + dim joins |
| "Que job mascarou esse paciente?" | Filtrar por dataset `silver.paciente_mascarado` |
| "Que jobs serão impactados se eu mudar `bronze.sih_sus`?" | Downstream lineage |
| "Quando esse dado foi atualizado pela última vez?" | `lastModifiedAt` no dataset |

### Integração com métricas

Métricas custom enviadas via OpenLineage:
- `record_count` no FacetField `outputStatistics`
- `bytes_written` idem
- Versão da transformação (`silver-1.2.3`)

## Custo / overhead

| Componente | Custo no laptop |
|---|---|
| Prometheus (scrape 15s) | ~100 MB RAM, 1% CPU |
| Grafana | ~150 MB RAM |
| Marquez | ~250 MB RAM |
| Postgres-marquez | ~150 MB RAM |
| Exporters (postgres, kafka) | ~50 MB total |
| **Total Observabilidade** | ~700 MB — cabe |

## Defesa em banca

| Pergunta | Resposta de 30s |
|---|---|
| Como vocês monitoram a solução? | Pirâmide RED+USE+DataOps. Prometheus coleta, Grafana exibe, alertas em Slack/PagerDuty por severidade. |
| Como vocês detectam que um job está degradando? | Métrica `pipeline_duration_seconds` — heatmap mostra distribuição; alerta se P95 cresce > 50% vs baseline. |
| Como rastrear de onde veio um dado? | Marquez/OpenLineage registra cada job + dataset de entrada e saída. Grafo navegável na UI. |
| O que vocês fazem se um schema mudar upstream? | Métrica `schema_drift_total` dispara alerta amarelo; PR é aberto para promover ou rejeitar. |
| Como saber se PII vazou para Silver? | Métrica `pii_in_silver_check_failed` deve ser sempre 0 — passa de 0 é alerta crítico, page imediato, pipeline pausado. |
| E se o consumer Kafka cair? | `kafka_consumer_lag` cresce → alerta. Supervisor (DAG) reinicia automaticamente em <5min. |
| Quanto tempo até detectar uma falha? | Scrape Prometheus 15s + avaliação 30s + roteamento 30s = ~1min para alerta crítico. |
| Como vocês evitam alert fatigue? | Severidade calibrada: crítico = page; alto = Slack útil; médio = log. Janela de silenciamento programada. |
| O que sobra para evolução? | Loki (logs centralizados), tracing distribuído (Jaeger), SLOs com error budgets. |

## Referências

- **Brendan Gregg — USE Method:** <https://www.brendangregg.com/usemethod.html>
- **Tom Wilkie — RED Method:** <https://thenewstack.io/monitoring-microservices-red-method/>
- **OpenLineage spec:** <https://openlineage.io/docs/spec/>
- **Marquez:** <https://marquezproject.ai/>
- **Prometheus best practices:** <https://prometheus.io/docs/practices/naming/>
- **Grafana provisioning:** <https://grafana.com/docs/grafana/latest/administration/provisioning/>
