# Contexto de Negócio — Case Academia Santander Data Engineering

> Documenta o problema de negócio, objetivos, domínios de dados, fontes e escopo técnico da plataforma.

---

## Sumário

- [1. Problema de Negócio](#1-problema-de-negócio)
- [2. Domínios de Dados e Fontes](#2-domínios-de-dados-e-fontes)
- [3. Casos de Uso e Insights Esperados](#3-casos-de-uso-e-insights-esperados)
- [4. Escopo Técnico — Componentes da Solução](#4-escopo-técnico--componentes-da-solução)
- [5. Profiles Docker Compose](#5-profiles-docker-compose)
- [6. Decisão sobre Fontes de Dados](#6-decisão-sobre-fontes-de-dados)

---

## 1. Problema de Negócio

O Brazil enfrenta desafios críticos na gestão de dados de saúde pública:

**Desafios de Integração:**
- Dados epidemiológicos dispersos em múltiplas fontes (DataSUS, IBGE, CNES, APIs governamentais)
- Formatos heterogêneos: CSV, JSON REST, banco relacional, eventos de streaming
- Ausência de visão integrada para análise epidemiológica cruzada

**Desafios de Privacidade (LGPD):**
- Dados de pacientes contêm PII (CPF, nome, data de nascimento, endereço)
- Necessidade de mascaramento antes de qualquer análise
- Rastreabilidade e auditoria obrigatórias

**Desafios de Confiabilidade:**
- Fontes externas offline ou instáveis na execução local
- Pipelines que falham precisam ser reexecutados sem corromper o dado
- Schema das APIs governamentais muda sem aviso

**Questões de negócio a responder:**

1. Qual é a distribuição geográfica de notificações de arboviroses (dengue, zika, chikungunya)?
2. Quais municípios apresentam maior vulnerabilidade epidemiológica?
3. Qual o perfil demográfico dos pacientes atendidos por tipo de agravo?
4. Como correlacionar cobertura vacinal com redução de internações?
5. Qual o throughput de atendimentos em tempo real por estabelecimento?

---

## 2. Domínios de Dados e Fontes

A plataforma implementa **Data Mesh** com 6 domínios de negócio (ver [ADR-009](./ADRS.md#adr-009--data-mesh-organização-por-domínios)):

| Domínio | Fonte | Tipo | Frequência |
|---|---|---|---|
| **epidemiologico** | API MS — dengue, zika, chikungunya (SINAN) | REST JSON | Mensal |
| **hospitalar** | API MS — SIM óbitos, CNES estabelecimentos | REST JSON | Mensal |
| **geografico** | API MS — municípios e regiões de saúde | REST JSON | Anual |
| **vacinal** | API MS — doses aplicadas PNI 2024 | REST JSON | Mensal |
| **oltp** | Postgres OLTP — cadastro de pacientes (Faker) | BD Relacional | Diário (snapshot) |
| **streaming** | Kafka — atendimentos em tempo real (Faker) | Stream | Contínuo |

**Mudança de domínio (2026-07-13):** fontes migradas de CSV/FTP DataSUS para **REST API pública** do Ministério da Saúde (`apidadosabertos.saude.gov.br`). Não requer autenticação. Cache offline em `data/raw/` garante funcionamento sem internet.

---

## 3. Casos de Uso e Insights Esperados

| Caso de Uso | Domínios Envolvidos | Camada Gold |
|---|---|---|
| Mapa de calor epidemiológico por município | epidemiologico + geografico | `fato_notificacao` × `dim_municipio` |
| Perfil etário e de gênero dos notificados | epidemiologico | `fato_notificacao` × `dim_tempo` |
| Ranking de estabelecimentos por volume de vacinação | vacinal + hospitalar | `fato_vacinacao` × `dim_estabelecimento` |
| Correlação cobertura vacinal × notificações | vacinal + epidemiologico | join em `dim_municipio` |
| Throughput de atendimentos por turno (streaming) | streaming | `fato_atendimento_stream` |
| Demonstração de mascaramento PII (LGPD) | oltp | `dim_paciente` SCD2 |

---

## 4. Escopo Técnico — Componentes da Solução

**Princípio orientador:** a avaliação considera **corretude técnica + clareza + reprodutibilidade**. Cada serviço extra cobra custo: RAM, tempo de boot, configuração e debug.

Critério de inclusão: **um serviço entra como obrigatório se cobre um requisito explícito do enunciado**.

### Núcleo obrigatório — Docker Compose

| Componente | Cobre requisito | Justificativa |
|---|---|---|
| **Postgres** (schemas: oltp, airflow, gold_dw) | Extração + Armazenamento | Source OLTP + backend Airflow + SCD2 Gold |
| **MinIO** | Armazenamento + Arquitetura | Data Lake S3-compatível; buckets `bronze/silver/gold` |
| **Kafka** (KRaft, sem Zookeeper) | Ingestão streaming | Backbone de streaming; tópico `notificacoes.raw` |
| **Airflow** | Ingestão + Arquitetura | Orquestração batch; `PythonOperator` + `SparkKubernetesOperator` |
| **stream-producer** (apps/streaming/producer) | Ingestão streaming | Faker → Kafka; processo contínuo separado do Airflow |
| **Hive Metastore** | Serving Layer | Catálogo thrift para Trino descobrir tabelas Delta no MinIO |
| **Trino** | Armazenamento + Arquitetura | Query federada sobre Delta Lake + RBAC por catálogo |
| **Prometheus** | Observabilidade | Coleta de métricas de todos os serviços |
| **Grafana** | Observabilidade | Dashboards provisionados automaticamente |
| **Marquez** (+OpenLineage) | Observabilidade | Lineage de dados — "rastrear fluxo de dados" |

### Núcleo obrigatório — Rancher Desktop (k3s)

> **Pré-requisito do avaliador:** Rancher Desktop instalado e rodando. Ver [ADR-007](./ADRS.md#adr-007--spark-on-kubernetes-rancher-desktop).

| Componente | Cobre requisito | Justificativa |
|---|---|---|
| **spark-on-k8s-operator** (Helm) | Ingestão + Escalabilidade | Gerencia SparkApplication CRDs no k3s local |
| **Spark driver + executors** (pods) | Ingestão + Escalabilidade | Processamento distribuído Bronze→Silver→Gold + Structured Streaming |

**Spark não entra no Docker Compose** — decisão formal em [ADR-007](./ADRS.md#adr-007--spark-on-kubernetes-rancher-desktop).

### Evolução Futura (fora do entregável)

| Componente | Por que está fora | Como aparece |
|---|---|---|
| **DataHub** | Pesado (vários containers Java), instável em Compose | Mencionado em docs/04-governanca-lgpd.md |
| **Apache Ranger** | RBAC coberto por Trino + Postgres roles | Mencionado como evolução cloud |
| **Loki** | Logs Docker cobrem o case; Loki sem requisito novo | Slide de evolução futura |
| **Vault** | `.env` + k8s Secrets cobre criptografia em repouso | Diagrama de arquitetura alvo cloud |

---

## 5. Profiles Docker Compose

| Profile | Serviços |
|---|---|
| `core` | postgres, minio, minio-init, airflow |
| `streaming` | kafka, stream-producer |
| `serving` | hive-metastore, trino, minio |
| `observability` | prometheus, grafana, marquez |

```bash
make up-core          # postgres + minio + airflow
make up-all           # todos os profiles → demo completo
make k8s-setup        # provisiona namespace spark + spark-operator no Rancher Desktop
make smoke            # testes E2E completos
make warmup           # docker pull de todas as imagens (10 min antes)
```

---

## 6. Decisão sobre Fontes de Dados

**Estratégia de cache offline:** depender de internet na execução local é risco inaceitável. `make seed` baixa e armazena amostras em `data/raw/` na primeira execução. Demos subsequentes usam cache offline.

| Fonte | Endpoint | Cache local |
|---|---|---|
| CNES estabelecimentos | `/cnes/estabelecimentos` | `data/raw/cnes/` |
| Dengue notificações | `/arboviroses/dengue?nu_ano=2024` | `data/raw/dengue/` |
| Zika notificações | `/arboviroses/zikavirus?nu_ano=2024` | `data/raw/zika/` |
| Chikungunya | `/arboviroses/chikungunya?nu_ano=2024` | `data/raw/chikungunya/` |
| SIM óbitos | `/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade` | `data/raw/sim/` |
| PNI vacinação | `/vacinacao/doses-aplicadas-pni-2024` | `data/raw/vacinacao/` |
| Municípios | `/macrorregiao-e-regiao-de-saude/municipio` | `data/raw/municipios/` |
| OLTP pacientes | Postgres `oltp` schema | Gerado por `make seed` (Faker) |
| Streaming atendimentos | Kafka `notificacoes.raw` | Gerado por `apps/streaming/producer` |

**Resumo de capacidade (revisado 2026-07-13):**

| Dimensão | Valor |
|---|---|
| Serviços Docker Compose | 10 (sem Spark) |
| Pods k8s (Rancher Desktop) | driver + 2 executors por job Spark |
| RAM Compose | ~8 GB |
| RAM Rancher Desktop | ≥ 8 GB alocados |
| Boot completo (`make up-all`) | ~5 min |
| Pontos de falha externa na demo | 0 (tudo cacheável) |
