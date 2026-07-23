# Data Lake Medalhão — Vigilância Epidemiológica

[![CI](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/ci.yml)
[![Smoke](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/smoke.yml/badge.svg)](https://github.com/Matheus-Fideles/case-data-master-data-engineer/actions/workflows/smoke.yml)

> **Status dos testes:** 104 unit tests ✅ | 36 smoke tests ✅ (3 skipped por design) | 0 falhas

Case técnico de Engenharia de Dados para a Academia Santander. Pipeline end-to-end que ingere dados de APIs públicas do Ministério da Saúde, processa via arquitetura Lambda (batch + streaming) e expõe análises epidemiológicas para suporte a decisão em saúde pública.

## Visão geral da arquitetura

```
REST API (apidadosabertos.saude.gov.br)
    │
    └──[PythonOperator · Airflow]──────────────────→  MinIO  landing/{domínio}/   (JSON)
                │
                └──[DockerOperator · local[2]]──────→  MinIO  bronze/{domínio}/   (Delta ACID)
                              │
                              └──[DockerOperator · local[2]]──────→  MinIO  silver/{domínio}/   (Delta + PII masking)
                                            │
                                            └──[DockerOperator · local[2]]──────→  MinIO  gold/{domínio}/   (Delta star schema)
                                                                                       │
                                                                          Hive Metastore (thrift:9083)
                                                                                       │
                                                                             Trino 448 (catalog: delta)

Faker container ──→ Kafka ──→ [Spark Structured Streaming · Docker] ──→ bronze/streaming/ ──→ gold/streaming/
```

### Data Mesh + Medallion

O projeto combina **Medallion Architecture** (Bronze → Silver → Gold, ADR-0001) com **Data Mesh** (ADR-0009):

| Domínio | Owner | Produtos de Dados |
|---|---|---|
| `epidemiologico` | team-epidemiologico | dengue, zika, chikungunya, notificacao, dim_paciente |
| `hospitalar` | team-hospitalar | sim-obitos, cnes-estabelecimentos |
| `geografico` | team-geografico | dim-municipio |
| `vacinal` | team-vacinal | vacinacao-pni, dim-vacina |
| `streaming` | team-streaming | atendimento-stream |

Cada domínio possui descritores YAML em `data-products/{domínio}/{produto}/dataproduct.yaml` com SLOs, schema contracts e lineage.

### Arquitetura Hexagonal (ADR-0010)

```
apps/{domínio}/
├── domain/         # entidades e serviços de negócio (sem deps de infra)
├── ports/          # interfaces (inbound/outbound)
├── adapters/       # implementações concretas
└── jobs/           # orquestradores: montam ports + adapters + executam
```

## Requisitos técnicos cobertos

| Requisito | Solução |
|---|---|
| **Extração de dados** | 7 endpoints REST + OLTP Faker + Kafka stream |
| **Ingestão** | Airflow DAGs (PythonOperator + DockerOperator (Spark local[2])) |
| **Armazenamento** | MinIO (Delta Lake) — todo o Gold no lake, não em Postgres |
| **Serving layer** | Trino 448 + Hive Metastore (thrift) |
| **Organização** | Data Mesh (domínios) + Arquitetura Hexagonal (Ports & Adapters) |
| **Observabilidade** | Prometheus + Grafana + Marquez (OpenLineage) |
| **Segurança de dados** | RBAC (Postgres roles + Trino ACL) + TLS (evolução) |
| **Mascaramento de dados** | SHA-256+salt (CPF), generalização (data nasc.), supressão (telefone) |
| **Arquitetura de dados** | Lambda + Star Schema + SCD Tipo 2 + Delta ACID |
| **Escalabilidade** | Spark DockerOperator (local[2]) → escalável via k8s em produção + mapeamento 1:1 para AWS (S3+EMR+MSK) |

## Pré-requisitos

| Ferramenta | Versão mínima | Instalação |
|---|---|---|
| Docker Desktop / Docker Engine | 24+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Python | 3.11+ | `brew install python@3.11` |
| `make` | qualquer | incluso no macOS (Xcode CLI Tools) |

RAM mínima recomendada:
- **Docker:** 8 GB alocados
- **Total:** 8 GB RAM

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
make up-serving             # Hive Metastore + Trino (serving layer via Delta Lake)
make up-observability       # Prometheus + Grafana + Marquez
```

Stop and reset:

```bash
make demo-stop              # stops containers, preserves volumes
make demo-reset             # wipes volumes and starts fresh
```

### Manual setup (full stack)

```bash
# 1. Initial setup
pip install pre-commit && pre-commit install
make seed                   # download API samples for offline mode

# 2. Infrastructure
make up-all                 # all Compose profiles (includes spark-local-setup)

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
| Hive Metastore | thrift://localhost:9083 | — (interno) |
| Trino UI | http://localhost:8085/ui | trino / (sem senha) |
| Metabase | http://localhost:3001 | admin@local.dev / Admin1234! |
| Grafana | http://localhost:3000 | admin / admin |
| Marquez UI | http://localhost:5000 | — |
| Prometheus | http://localhost:9090 | — |

### Pré-aquecimento do ambiente

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
| `make up-serving` | Add Hive Metastore + Trino |
| `make up-observability` | Add Prometheus + Grafana + Marquez |
| `make up-all` | Start all Compose profiles |
| `make down` | Stop and remove all containers |
| `make smoke` | Full smoke test suite |
| `make smoke-min` | Minimal smoke: infra + bronze + masking + idempotency + security |
| `make seed` | Download API samples to `data/raw/` (offline mode) |
| `make spark-image-local` | Build local Spark image (spark-custom:3.5-delta) |
| `make run-demo-pipeline` | Manually trigger E2E pipeline via Airflow CLI |
| `make warmup` | Pré-pull de imagens Docker + k8s |
| `make logs` | Tail logs from all services |

## Estrutura do repositório

```
.
├── docs/                        # Documentação
│   ├── 01-contexto-negocio.md   # Problema, domínios, fontes e escopo técnico
│   ├── 02-arquitetura.md        # Stack, padrões, modelo dimensional e matriz comparativa
│   ├── 03-integrações.md        # Resumo das fontes de dados (índice para integrations/)
│   ├── 04-governanca-lgpd.md    # LGPD, mascaramento PII, RBAC e padrões de código
│   ├── 05-observabilidade-sre.md # Prometheus, Grafana, Marquez, runbook e smoke tests
│   ├── ADRS.md                  # Todas as 12 decisões arquiteturais (ADR-001 a ADR-012)
│   ├── data-dictionary.md       # Dicionário coluna-a-coluna das tabelas Gold
│   ├── assets/                  # Diagramas PNG (arquitetura, fluxo, stack tecnológica)
│   ├── integrations/            # Guias detalhados por fonte (CNES, SINAN, PNI, OLTP, Kafka)
│   └── specs/                   # Specs técnicas: DAGs Airflow, schemas Bronze, smoke tests
├── apps/                        # Código por domínio de negócio (Data Mesh + Hexagonal)
│   ├── shared/                  # Kernel compartilhado: spark, masking, lineage, base classes
│   ├── epidemiologico/          # Dengue, Zika, Chikungunya, SINAN
│   ├── hospitalar/              # CNES, SIM (óbitos)
│   ├── geografico/              # Municípios IBGE
│   ├── vacinal/                 # PNI doses aplicadas
│   ├── streaming/
│   │   ├── producer/            # Faker → Kafka (container Docker standalone)
│   │   └── jobs/                # Spark Structured Streaming consumer (Docker)
│   └── oltp/                    # Snapshot Postgres OLTP (PII source)
├── data-products/               # Descritores Data Mesh por domínio
│   ├── epidemiologico/          # dengue/, notificacao/, paciente/
│   ├── hospitalar/              # sim-obitos/, cnes/
│   ├── geografico/              # municipios/
│   ├── vacinal/                 # vacinacao-pni/
│   └── streaming/               # atendimento/
├── airflow/dags/                # DAGs Airflow (bronze/, silver/, gold/, quality/, maint/)
├── infra/                       # Configs: Trino, Grafana, Prometheus, Postgres, Hive Metastore
│   └── hive-metastore/          # Dockerfile + hive-site.xml (HMS standalone)
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

Com `OFFLINE_MODE=1`, todos os extratores leem de `data/raw/` em vez de chamar as APIs. Garante reprodutibilidade absoluta em qualquer ambiente sem conectividade externa.

## Segurança e LGPD

Ver `docs/04-governanca-lgpd.md` para detalhes completos. Resumo:

- **CPF**: SHA-256 + salt (salt em variável de ambiente, nunca no código)
- **Nome**: substituído por valor Faker (pseudonimização)
- **Data de nascimento**: generalizada para apenas o ano
- **CEP**: truncado para os 3 primeiros dígitos
- **Telefone/e-mail**: suprimidos

Mascaramento ocorre na transição **Bronze → Silver**. Gold nunca vê PII.

## Decisões de arquitetura (ADRs)

| ADR | Decisão |
|---|---|
| [0001](docs/ADRS.md#adr-001--arquitetura-lambda-vs-kappa) | Lambda vs Kappa → **Lambda** |
| [0002](docs/ADRS.md#adr-002--estratégia-de-idempotência-e-merge) | Idempotência → **MERGE por NK** |
| [0003](docs/ADRS.md#adr-003--política-de-schema-evolution) | Schema evolution → **mergeSchema=true** |
| [0004](docs/ADRS.md#adr-004--estratégia-de-mascaramento-de-pii) | Mascaramento PII → **SHA-256+salt no Silver** |
| [0005](docs/ADRS.md#adr-005--watermark-e-tratamento-de-eventos-atrasados) | Watermark → **1 hora** |
| [0006](docs/ADRS.md#adr-006--implementação-de-scd-tipo-2) | SCD Tipo 2 → **dt_inicio/dt_fim + is_current** |
| [0007](docs/ADRS.md#adr-007--spark-on-kubernetes-rancher-desktop) | Spark → **DockerOperator local[2]** (supersede k3s) |
| [0008](docs/ADRS.md#adr-008--separação-de-camadas-de-ingestão) | Ingestão → **Python extrai, Spark transforma** |
| [0009](docs/ADRS.md#adr-009--data-mesh-organização-por-domínios) | Organização → **Data Mesh por domínio de negócio** |
| [0010](docs/ADRS.md#adr-010--arquitetura-hexagonal-ports--adapters) | Design → **Hexagonal (Ports & Adapters)** |
| [0011](docs/ADRS.md#adr-011--serving-layer-trino--hms--delta-lake) | Serving → **Trino + Hive Metastore + Delta Lake** |
| [0012](docs/ADRS.md#adr-0012--spark-via-dockeroperator-local2-substitui-adr-0007) | Spark exec → **DockerOperator (local[2])** |

## Escalabilidade para cloud

A arquitetura é desenhada com mapeamento 1:1 para AWS — migração é configuração, não rewrite:

| Local (Compose + Docker) | AWS equivalente |
|---|---|
| MinIO (Delta Lake) | S3 + Delta Lake OSS |
| Hive Metastore (thrift) | AWS Glue Data Catalog |
| Kafka KRaft | MSK Serverless |
| DockerOperator local[2] | EMR Serverless / EKS |
| Airflow | MWAA |
| Postgres | RDS Aurora PostgreSQL |
| Trino | Athena / Trino on EKS |
| Marquez (OpenLineage) | AWS Glue Data Catalog + DataZone |
| Prometheus + Grafana | CloudWatch + Managed Grafana |

Ver `docs/02-arquitetura.md` §7 para a análise completa de cloud vs. on-premises.
