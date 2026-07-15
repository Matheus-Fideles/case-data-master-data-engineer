# Data Lake Medalhão — Vigilância Epidemiológica

[![CI](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/ci.yml)
[![Smoke](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/smoke.yml/badge.svg)](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/smoke.yml)

> **Status dos testes:** 104 unit tests ✅ | 36 smoke tests ✅ (3 skipped por design) | 0 falhas

Case técnico de Engenharia de Dados para a Academia Santander. Pipeline end-to-end que ingere dados de APIs públicas do Ministério da Saúde, processa via arquitetura Lambda (batch + streaming) e expõe análises epidemiológicas para suporte a decisão em saúde pública.

## Visão geral da arquitetura

```
REST API (apidadosabertos.saude.gov.br)
    │
    └──[PythonOperator · Airflow]──────────────────→  MinIO  landing/   (JSON)
                │
                └──[SparkKubernetesOperator · k3s]──→  MinIO  bronze/   (Delta ACID)
                              │
                              └──[SparkKubernetesOperator · k3s]──→  MinIO  silver/   (Delta + PII masking)
                                            │
                                            └──[SparkKubernetesOperator · k3s]──→  MinIO  gold/   (Delta star schema)
                                                                                       │
                                                                             Postgres  gold_dw ←→ Trino ←→ Metabase

Faker container ──→ Kafka ──→ [Spark Structured Streaming · k3s] ──→ bronze/stream/ ──→ gold/
```

## Requisitos técnicos cobertos

| Requisito | Solução |
|---|---|
| **Extração de dados** | 7 endpoints REST + OLTP Faker + Kafka stream |
| **Ingestão** | Airflow DAGs (PythonOperator + SparkKubernetesOperator) |
| **Armazenamento** | MinIO (Delta Lake) + Postgres (Gold DW) |
| **Observabilidade** | Prometheus + Grafana + Marquez (OpenLineage) |
| **Segurança de dados** | RBAC (Postgres roles + Trino ACL) + TLS (evolução) |
| **Mascaramento de dados** | SHA-256+salt (CPF), generalização (data nasc.), supressão (telefone) |
| **Arquitetura de dados** | Lambda + Star Schema + SCD Tipo 2 + Delta ACID |
| **Escalabilidade** | Spark on Kubernetes + mapeamento 1:1 para AWS (S3+EMR+MSK) |

## Pré-requisitos

| Ferramenta | Versão mínima | Instalação |
|---|---|---|
| Docker Desktop / Docker Engine | 24+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Rancher Desktop (k3s) | 1.12+ | [rancherdesktop.io](https://rancherdesktop.io) |
| `kubectl` | 1.28+ | incluso no Rancher Desktop |
| Helm | 3.14+ | `brew install helm` |
| Python | 3.11+ | `brew install python@3.11` |
| `make` | qualquer | incluso no macOS (Xcode CLI Tools) |

RAM mínima recomendada:
- **Docker Compose:** 8 GB alocados
- **Rancher Desktop (k3s):** 8 GB alocados
- **Total:** 16 GB RAM

## Quick start

### One command (recommended)

```bash
git clone <repo-url> && cd case-data-master-data-engineer
cp .env.example .env        # edit PII_SALT if needed
make demo                   # starts Postgres + MinIO + Airflow, runs smoke tests, opens browser
```

`make demo` takes ~3 min on first run (image pulls). On success:

| Interface | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

Then trigger the full pipeline:

```bash
make run-demo-pipeline      # triggers bronze → silver → gold DAGs via Airflow CLI
```

Add serving / observability layers:

```bash
make up-serving             # Trino + Metabase (bloqueia ~30 min no 1º boot para migrações Liquibase)
make up-observability       # Prometheus + Grafana + Marquez
```

> **Nota Metabase:** `make up-serving` aguarda o Metabase ficar pronto e cria o admin automaticamente com as credenciais de `.env` (`MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD`). No primeiro boot, leva ~30 minutos para completar 367 migrações de banco. Em boots subsequentes, fica pronto em ~2 min.

Stop and reset:

```bash
make demo-stop              # stops containers, preserves volumes
make demo-reset             # wipes volumes and starts fresh
```

### Manual setup (full stack + Kubernetes)

```bash
# 1. Initial setup
pip install pre-commit && pre-commit install
make seed                   # download API samples for offline mode

# 2. Infrastructure
make k8s-check              # verify Rancher Desktop is running
make up-all                 # all Compose profiles
make k8s-setup              # spark-operator + namespace + secrets
make k8s-spark-image        # build + push custom Spark image

# 3. Airflow
make airflow-setup          # connections, variables, pools
make run-demo-pipeline      # trigger bronze → silver → gold

# 4. Smoke tests
make smoke                  # full suite (requires compose up)
```

### All services

| Interface | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino UI | http://localhost:8085/ui | trino / (no password) |
| Metabase | http://localhost:3001 | admin@local.dev / Admin1234! (configurado via `make metabase-setup`) |
| Grafana | http://localhost:3000 | admin / admin |
| Marquez UI | http://localhost:5000 | — |
| Prometheus | http://localhost:9090 | — |

### Pre-demo warmup (10 min before presentation)

```bash
make warmup   # pre-pull Docker + k8s images
```

## Targets Makefile

| Target | Description |
|---|---|
| `make demo` | **One command**: start core services + Airflow setup + smoke tests + open browser |
| `make demo-stop` | Stop demo containers (preserves volumes for next run) |
| `make demo-reset` | Wipe volumes and restart a fresh demo |
| `make up-core` | Start postgres + minio + airflow (core profile only) |
| `make up-streaming` | Add Kafka + stream-producer |
| `make up-serving` | Add Trino + Metabase |
| `make up-observability` | Add Prometheus + Grafana + Marquez |
| `make up-all` | Start all Compose profiles |
| `make down` | Stop and remove all containers |
| `make smoke` | Full smoke test suite |
| `make smoke-min` | Minimal smoke: infra + bronze + masking + idempotency + security |
| `make seed` | Download API samples to `data/raw/` (offline mode) |
| `make k8s-check` | Verify Rancher Desktop connectivity |
| `make k8s-setup` | Install spark-operator + create spark namespace + secrets |
| `make k8s-spark-image` | Build + push custom Spark image to local registry |
| `make run-demo-pipeline` | Manually trigger E2E pipeline via Airflow CLI |
| `make warmup` | Pre-pull Docker + k8s images |
| `make logs` | Tail logs from all services |

## Estrutura do repositório

```
.
├── docs/                        # Arquitetura e especificações
│   ├── architecture/
│   │   ├── decisions/           # ADRs (0001–0008)
│   │   ├── diagrams/            # Diagramas de solução, fluxo e deployment
│   │   ├── data-model.md        # Star schema dimensional (fatos + dims + SCD2)
│   │   ├── comparative-matrix.md # Por que X e não Y (Delta vs Iceberg, Spark vs Flink, etc.)
│   │   └── scope.md             # Componentes obrigatórios vs. evolução futura
│   ├── specs/
│   │   ├── airflow-dags.md      # Especificação de todos os DAGs (30 DAGs)
│   │   └── bronze-schemas.md    # Schemas das 9 tabelas Bronze (StructType)
│   ├── standards/
│   │   └── pre-commit.md        # Padrão de qualidade de código (ruff, detect-secrets, anti-PII)
│   ├── integrations/            # Documentação de cada fonte de dados
│   ├── security.md              # LGPD + mascaramento PII + RBAC
│   └── observability.md         # Prometheus + Grafana + Marquez (OpenLineage)
├── airflow/dags/                # DAGs Airflow (bronze/, silver/, gold/, quality/, maint/)
├── pipelines/                   # Código dos jobs Spark + extratores Python
│   ├── extraction/              # PythonOperator → landing/  (API REST → MinIO)
│   ├── batch/                   # SparkKubernetesOperator → bronze/silver/gold
│   ├── streaming/               # Spark Structured Streaming (Kafka → gold)
│   └── common/                  # masking.py (SHA-256+salt) e utilitários
├── stream-producer/             # Faker → Kafka (container Python standalone)
├── infra/                       # Configs dos serviços (Trino, Grafana, Prometheus, Postgres init)
├── k8s/                         # SparkApplication YAMLs + namespace/RBAC k8s
├── data/raw/                    # Cache offline de APIs (gitignored — gerado por make seed)
└── tests/                       # Testes unitários (extraction, batch) e smoke tests
```

## Fontes de dados

Todas as fontes são REST APIs públicas do Ministério da Saúde — sem autenticação necessária.

| Fonte | Endpoint | Destino |
|---|---|---|
| Dengue | `/arboviroses/dengue` | `bronze.dengue` |
| Zika | `/arboviroses/zikavirus` | `bronze.zika` |
| Chikungunya | `/arboviroses/chikungunya` | `bronze.chikungunya` |
| SIM Óbitos | `/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade` | `bronze.sim_obitos` |
| Vacinação PNI | `/vacinacao/doses-aplicadas-pni-2024` | `bronze.vacinacao_pni` |
| CNES | `/cnes/estabelecimentos` | `bronze.cnes_estabelecimentos` |
| Municípios | `/macrorregiao-e-regiao-de-saude/municipio` | `bronze.municipios` |

Dados sintéticos gerados localmente:
- **OLTP Faker**: pacientes fictícios no Postgres (schema `oltp`) para demonstração de PII masking
- **Streaming**: Faker → Kafka (`notificacoes.raw`) para pipeline de tempo real

## Modo offline (demo sem internet)

```bash
OFFLINE_MODE=1 make run-demo-pipeline
```

Com `OFFLINE_MODE=1`, todos os extratores leem de `data/raw/` em vez de chamar as APIs. Garante reprodutibilidade absoluta no dia da apresentação.

## Segurança e LGPD

Ver `docs/security.md` para detalhes completos. Resumo:

- **CPF**: SHA-256 + salt (salt em variável de ambiente, nunca no código)
- **Nome**: substituído por valor Faker (pseudonimização)
- **Data de nascimento**: generalizada para apenas o ano
- **CEP**: truncado para os 3 primeiros dígitos
- **Telefone/e-mail**: suprimidos

Mascaramento ocorre na transição **Bronze → Silver**. Gold nunca vê PII.

## Decisões de arquitetura (ADRs)

| ADR | Decisão |
|---|---|
| [0001](docs/architecture/decisions/0001-lambda-vs-kappa.md) | Lambda vs Kappa → **Lambda** |
| [0002](docs/architecture/decisions/0002-idempotency-merge.md) | Idempotência → **MERGE por NK** |
| [0003](docs/architecture/decisions/0003-schema-evolution.md) | Schema evolution → **mergeSchema=true** |
| [0004](docs/architecture/decisions/0004-pii-masking.md) | Mascaramento PII → **SHA-256+salt no Silver** |
| [0005](docs/architecture/decisions/0005-streaming-watermark.md) | Watermark → **1 hora** |
| [0006](docs/architecture/decisions/0006-scd2.md) | SCD Tipo 2 → **dt_inicio/dt_fim + is_current** |
| [0007](docs/architecture/decisions/0007-spark-on-kubernetes.md) | Spark → **k3s via Rancher Desktop** |
| [0008](docs/architecture/decisions/0008-ingestion-layers.md) | Ingestão → **Python extrai, Spark transforma** |

## Escalabilidade para cloud

A arquitetura é desenhada com mapeamento 1:1 para AWS — migração é configuração, não rewrite:

| Local (Compose + k3s) | AWS equivalente |
|---|---|
| MinIO | S3 |
| Kafka KRaft | MSK Serverless |
| Spark on k3s | EMR Serverless / EKS |
| Airflow | MWAA |
| Postgres | RDS Aurora PostgreSQL |
| Marquez | AWS Glue Data Catalog + DataZone |
| Prometheus + Grafana | CloudWatch + Managed Grafana |

Ver `docs/architecture/comparative-matrix.md` §7 para a análise completa de cloud vs. on-premises.
