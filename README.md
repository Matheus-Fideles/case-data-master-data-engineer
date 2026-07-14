# Data Lake Medalhão — Vigilância Epidemiológica

[![CI](https://github.com/matheusfideles/case-data-master-data-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/matheusfideles/case-data-master-data-engineer/actions/workflows/ci.yml)
[![Smoke](https://github.com/matheusfideles/case-data-master-data-engineer/actions/workflows/smoke.yml/badge.svg)](https://github.com/matheusfideles/case-data-master-data-engineer/actions/workflows/smoke.yml)

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

### 1. Configuração inicial

```bash
git clone <repo-url> && cd case-data-master-data-engineer

# Copiar variáveis de ambiente (editar se necessário para demo)
cp .env.example .env

# Instalar hooks de qualidade de código
pip install pre-commit
pre-commit install

# Baixar amostras de dados para modo offline (demo sem internet)
make seed
```

### 2. Subir infraestrutura

```bash
# Verificar que Rancher Desktop está rodando
make k8s-check

# Subir todos os serviços Docker Compose
make up-all

# Instalar spark-operator no k3s + criar namespace spark
make k8s-setup

# Build e push da imagem Spark customizada para o registry local
make k8s-spark-image

# Aguardar todos os serviços ficarem healthy (~3-5 min)
make smoke
```

### 3. Configurar Airflow e executar pipeline (demo)

```bash
# Criar conexões, variáveis e pools no Airflow
make airflow-setup

# Acessar Airflow UI
open http://localhost:8080
# user: admin / pass: admin

# Trigger manual do pipeline completo:
# dag_bronze_dengue → dag_silver_notificacao → dag_gold_fato_notificacao
# Ou via CLI:
make run-demo-pipeline
```

### 4. Verificar resultados

| Interface | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino UI | http://localhost:8085/ui | trino / (sem senha) |
| Metabase | http://localhost:3001 | admin@local.dev / admin123 |
| Grafana | http://localhost:3000 | admin / admin |
| Marquez UI | http://localhost:5000 | — |
| Prometheus | http://localhost:9090 | — |

### 5. Preparação para demo ao vivo (10 min antes)

```bash
make warmup   # pull de imagens Docker + k8s images antecipadamente
```

## Targets Makefile

| Target | O que faz |
|---|---|
| `make up-core` | Sobe postgres + minio + airflow (sem streaming/serving) |
| `make up-all` | Sobe todos os profiles Compose |
| `make down` | Para e remove todos os containers |
| `make smoke` | Health checks em todos os endpoints |
| `make seed` | Baixa amostras de APIs e salva em `data/raw/` |
| `make k8s-check` | Verifica conectividade com Rancher Desktop |
| `make k8s-setup` | Instala spark-operator + cria namespace spark |
| `make k8s-spark-image` | Build + push imagem Spark customizada |
| `make run-demo-pipeline` | Trigger manual do pipeline E2E via Airflow CLI |
| `make warmup` | Pré-aquece imagens Docker + k8s |
| `make logs` | Tails de logs de todos os serviços |

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
| [0006](docs/architecture/decisions/0006-scd2.md) | SCD Tipo 2 → **valid_from/to + is_current** |
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
