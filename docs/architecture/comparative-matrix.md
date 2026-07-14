# Matriz Comparativa de Decisões Tecnológicas

- **Status:** Aceito
- **Data:** 2026-05-07
- **Propósito:** documentar as alternativas avaliadas e justificar as escolhas. Este documento é a **defesa frontal contra perguntas previsíveis da banca**: *"Por que X e não Y?"*. Cada matriz termina com a **decisão final** e o **veredito para a defesa oral**.

## Índice

1. [Formato de tabela do data lake](#1-formato-de-tabela-do-data-lake) — Delta vs Iceberg vs Hudi
2. [Streaming backbone](#2-streaming-backbone) — Kafka vs Pulsar vs Kinesis
3. [Data warehouse Gold](#3-data-warehouse-gold) — Postgres vs ClickHouse vs DuckDB vs BigQuery
4. [Engine de processamento](#4-engine-de-processamento) — Spark vs Flink vs Polars/Dask
5. [Orquestração](#5-orquestração) — Airflow vs Dagster vs Prefect
6. [Catálogo / lineage](#6-catálogo--lineage) — Marquez vs DataHub vs OpenMetadata
7. [Cloud vs on-premises](#7-cloud-vs-on-premises) — diferenciação volume/velocidade/variedade
8. [Arquitetura Lambda vs Kappa](#8-arquitetura-lambda-vs-kappa) — referência ao ADR

---

## 1. Formato de tabela do data lake

| Critério | **Delta Lake** ✅ | Iceberg | Hudi |
|---|---|---|---|
| Maturidade na comunidade Spark | Maior (Databricks-driven) | Crescente, multi-engine | Boa, voltado a CDC |
| ACID + time travel | ✅ | ✅ | ✅ |
| MERGE / UPSERT | ✅ nativo, sintaxe SQL clara | ✅ via row-level updates | ✅ COPY ON WRITE / MERGE ON READ |
| Schema evolution | ✅ `mergeSchema=true` | ✅ explícita por ALTER | ✅ |
| Integração Spark Structured Streaming | Nativa (mais simples) | Boa (requer config extra) | Nativa |
| Integração Trino | ✅ via Trino Delta connector | ✅ nativa, melhor performance | ✅ via connector |
| Z-ORDER / clustering | ✅ `OPTIMIZE ... ZORDER BY` | ✅ partition transforms (mais flexível) | ✅ clustering |
| Curva de aprendizado | Baixa | Média | Alta |
| Ecossistema multi-engine | Spark-first; Trino/Flink suportados | Multi-engine de origem (Spark, Flink, Trino, Snowflake) | Spark-first |

**Decisão:** **Delta Lake**.

**Defesa para banca:** *"Escolhi Delta pela maturidade da integração com Spark Structured Streaming e pela sintaxe MERGE direta, que simplifica idempotência e SCD2. Iceberg seria escolha igualmente sólida — a vantagem dele é multi-engine real e partition transforms mais flexíveis. Para o volume e prazo deste case, Delta entrega o mesmo resultado com menor risco de configuração."*

**Quando eu escolheria Iceberg:** se o cenário exigisse Snowflake ou BigQuery lendo o lake nativamente, ou se o volume justificasse hidden partitioning sofisticado.

---

## 2. Streaming backbone

| Critério | **Kafka** ✅ | Pulsar | Kinesis (AWS) |
|---|---|---|---|
| Padrão de mercado | Dominante | Crescente | Dominante na AWS |
| Operabilidade local | KRaft mode (sem Zookeeper) — simples | Mais complexo | Não roda local |
| Throughput em laptop | Alto, suficiente para demo | Alto | N/A |
| Tiered storage | Suportado (KIP-405) | Nativo de origem | Nativo |
| Multi-tenancy | Manual (ACLs) | Nativo | Account-level |
| Ecossistema (Spark, Connect, Schema Registry) | Maturíssimo | Bom | Limitado fora AWS |

**Decisão:** **Kafka** (KRaft, sem Zookeeper).

**Defesa:** *"Kafka é o padrão da indústria, integra direto com Spark Structured Streaming e roda localmente em modo KRaft sem Zookeeper, simplificando o Compose. Para a versão cloud, MSK é a evolução natural; Kinesis seria considerado em ambiente AWS-native sem Kafka legado."*

---

## 3. Data warehouse Gold

| Critério | **Postgres** ✅ | ClickHouse | DuckDB | BigQuery |
|---|---|---|---|---|
| Tipo | OLTP/HTAP relacional | OLAP colunar | OLAP embarcado (single-node) | OLAP serverless |
| Performance em fato dimensional médio (~GB) | Boa com índices BRIN | Excelente | Excelente | Excelente |
| RBAC granular | ✅ roles + RLS | ✅ | Limitado | ✅ IAM |
| Setup local | Trivial | Médio | Trivial (lib) | Não roda local |
| Custo de operação | Baixo | Médio | Mínimo | Pay-per-query |
| Adequação ao escopo do case | Justa (volume controlado, RBAC necessário) | Sobra de capacidade | Falta servidor para múltiplos consumidores | Não cabe em demo offline |

**Decisão:** **Postgres** como Gold.

**Defesa:** *"O volume da camada Gold neste case é da ordem de centenas de MB a poucos GB — bem dentro do que Postgres lida com folga, especialmente com índices apropriados em fatos particionados por ano_mes. Postgres traz RBAC maduro com Row-Level Security, que é exigido pelo requisito de segurança. ClickHouse seria a escolha óbvia se o volume fosse 100x maior; BigQuery se fôssemos cloud-native; DuckDB se fosse análise single-user."*

**Defesa contra "ClickHouse seria mais rápido":** *"Sim, em throughput de queries analíticas. Mas para este case o gargalo não é query speed do Gold — é a infraestrutura local de demo e o requisito de RBAC. Postgres atende com menos complexidade operacional."*

---

## 4. Engine de processamento

| Critério | **Spark** ✅ | Flink | Polars / Dask |
|---|---|---|---|
| Batch | ✅ classe A | ✅ via DataStream batch | ✅ (Polars excelente single-node) |
| Streaming | ✅ Structured Streaming (micro-batch) | ✅ true streaming, baixa latência | Limitado |
| Distribuído real | ✅ | ✅ | Polars: não. Dask: sim. |
| Curva de aprendizado | Média | Alta | Baixa |
| Integração Delta + Kafka | Nativa | Boa, mais setup | Limitada |
| Comunidade no contexto banco | Universal | Crescente | Pequena ainda |

**Decisão:** **Spark** (PySpark + Structured Streaming).

**Defesa:** *"Spark cobre batch e streaming com a mesma API e tem integração nativa com Delta e Kafka. Flink seria a escolha se a latência exigisse milissegundos e o caso fosse 100% streaming — não é o caso aqui. Polars é excelente para single-node, mas o requisito de processamento distribuído pede Spark."*

**Defesa contra "Flink tem latência menor":** *"Verdade. Spark Structured Streaming opera em micro-batches de 30s a 1s, suficiente para o caso de saúde pública aqui simulado. Para alta-frequência financeira ou IoT industrial, Flink ganharia."*

---

## 5. Orquestração

| Critério | **Airflow** ✅ | Dagster | Prefect |
|---|---|---|---|
| Adoção em bancos brasileiros | Dominante | Crescente | Pequena |
| Modelo conceitual | DAG estática | Software-defined assets (lineage nativo) | Flow dinâmico |
| Integração OpenLineage | ✅ via provider | ✅ nativo (asset lineage) | ✅ via plugin |
| Curva de entrada | Conhecida pela banca | Maior | Baixa |
| UI | Madura, completa | Moderna, mais limpa | Moderna |

**Decisão:** **Airflow 2.x**.

**Defesa:** *"Airflow é o padrão de fato em orquestração no contexto de bancos brasileiros, o que reduz fricção de onboarding e maximiza reuso de provider operators. Dagster tem um modelo conceitual mais elegante (assets), e em um projeto greenfield consideraria sério — para este case, o critério de aderência ao mercado-alvo da vaga pesou."*

---

## 6. Catálogo / lineage

| Critério | **Marquez (OpenLineage)** ✅ | DataHub | OpenMetadata |
|---|---|---|---|
| Foco | Lineage operacional puro | Catálogo full + lineage + governance | Catálogo + governance + colaboração |
| Footprint Compose | Leve (~512 MB) | Pesado (Elasticsearch+Kafka+MySQL+GMS) | Médio |
| Tempo de boot local | <30 s | 3–5 min | 1–2 min |
| Cobertura de requisito 4 (rastreio) | ✅ direta | ✅ + extras | ✅ + extras |
| UI de busca de tabelas | Limitada | Excelente | Excelente |

**Decisão:** **Marquez** no Compose obrigatório; **DataHub** como evolução demonstrada por screenshots.

**Defesa:** *"Marquez cobre o requisito 4 (rastreabilidade do fluxo) com footprint que cabe em laptop sem competir por RAM com Spark e Kafka. DataHub é a escolha de produção quando se quer governança plena, mas seu Compose tem 6+ containers JVM — não consegui justificar dentro do budget de 1h30 de demo. Solução: rodei DataHub em separado, capturei screenshots da UI e incluí no slide de evolução."*

---

## 7. Cloud vs on-premises

Diferenciação obrigatória pelo requisito 3 (volume × velocidade × variedade) e requisito 5 (segurança/escalabilidade).

| Dimensão | Stack local (Compose) | Equivalente AWS | Equivalente GCP | Equivalente Azure |
|---|---|---|---|---|
| Object storage | MinIO | S3 | GCS | ADLS Gen2 |
| Streaming | Kafka KRaft | MSK Serverless / Kinesis | Pub/Sub | Event Hubs |
| Compute distribuído | Spark Standalone | EMR / EMR Serverless / Glue | Dataproc / Dataflow | Synapse Spark / HDInsight |
| Orquestração | Airflow | MWAA | Cloud Composer | Data Factory |
| DW | Postgres | Redshift / Aurora | BigQuery | Synapse SQL |
| Catalog | Marquez | Glue Catalog + DataZone | Dataplex | Purview |
| Secrets | .env + Docker secrets | Secrets Manager / SSM | Secret Manager | Key Vault |
| Observability | Prometheus + Grafana | CloudWatch + Managed Grafana | Cloud Monitoring | Azure Monitor |
| RBAC | Postgres roles + Trino | IAM + Lake Formation | IAM + BQ row-level | RBAC + Purview |

**Trade-offs explicitados:**

| | Volume | Velocidade | Variedade |
|---|---|---|---|
| **On-prem (Compose, MinIO+Spark+Postgres)** | Limitado pelo hardware do nó; bom até dezenas de TB com cluster real | Bom; latência baixa intra-rack | Boa; suporta arquivos, BD, stream |
| **Cloud (S3+EMR+Redshift)** | Praticamente ilimitado; pay-per-GB | Excelente com auto-scaling | Excelente; serviços gerenciados por tipo |

**Decisão:** **Local-first com Docker Compose para o entregável**, com **arquitetura cloud descrita no slide final** como evolução.

**Defesa:** *"Local-first elimina dependência de internet/conta cloud no dia da demo, garantindo reprodutibilidade absoluta. A arquitetura é desenhada com mapeamento 1:1 para AWS, evidenciando que a migração para cloud é configuração, não rewrite — exatamente o que se espera de uma arquitetura cloud-ready."*

---

## 8. Arquitetura Lambda vs Kappa

Detalhada em [`decisions/0001-lambda-vs-kappa.md`](./decisions/0001-lambda-vs-kappa.md).

**Resumo da decisão:** Lambda. Razões: API pública de saúde publica dados com latência de horas/dias — forçar Kappa seria artificial; Lambda demonstra dois pipelines distintos vivos na demo; resiliência por isolamento.

---

## 9. Spark Standalone vs Spark on Kubernetes

Detalhado em [`decisions/0007-spark-on-kubernetes.md`](./decisions/0007-spark-on-kubernetes.md).

| Critério | Spark Standalone (Compose) | **Spark on k8s (Rancher Desktop)** ✅ |
|---|---|---|
| Realismo de produção | Baixo | **Alto — padrão de mercado** |
| Isolamento por job | Compartilhado | **Pod por job** |
| Escalabilidade horizontal | Manual (mais containers) | **Automático (executor pods)** |
| Integração Airflow | `SparkSubmitOperator` | **`SparkKubernetesOperator`** |
| Declaratividade | `spark-submit` flags | **`SparkApplication` YAML** |
| Portabilidade (EKS, GKE, AKS) | Nenhuma | **Total — mesmo YAML** |

**Decisão:** Spark on Kubernetes via Rancher Desktop. Airflow (no Compose) submete `SparkApplication` CRDs via `SparkKubernetesOperator`. Spark standalone não entra no Compose.

---

## 10. Spark Structured Streaming vs Apache Flink

| Critério | **Spark Structured Streaming** ✅ | Apache Flink |
|---|---|---|
| Latência | Micro-batch (1s–30s) | True streaming (ms) |
| Latência necessária (vigilância epidemiológica) | **Segundos/minutos bastam** | Overkill |
| Engine batch | **Mesmo Spark já no k8s** | Cluster separado |
| Delta Lake MERGE | **`foreachBatch` nativo** | Conector adicional |
| API reutilizável com batch | **Sim — mesmo SparkSession** | Não (DataStream API diferente) |
| Operação no k8s | **Mesmo spark-operator** | Flink Kubernetes Operator separado |
| Complexidade operacional | Baixa | Alta |

**Decisão:** Spark Structured Streaming. A latência de vigilância epidemiológica não exige milissegundos. Manter um único engine (Spark) para batch e streaming reduz complexidade operacional sem sacrificar funcionalidade para este domínio.

**Quando Flink seria a escolha:** alertas em tempo real de surto com latência < 1s, análise de padrões complexos de sequência de eventos (CEP), ou casos onde o volume de streaming é muito superior ao batch. Documentado como evolução futura na seção de melhorias.

---

## Apêndice — perguntas previsíveis e onde estão respondidas

| Pergunta | Resposta neste documento |
|---|---|
| Spark standalone ou k8s? | §9 + ADR 0007 |
| Structured Streaming ou Flink? | §10 |
| Por que Delta e não Iceberg? | §1 |
| Kafka ou Kinesis? | §2 |
| Como modelou o Gold? Postgres ou ClickHouse? | §3 |
| Spark ou Flink? | §4 |
| Airflow ou Dagster? | §5 |
| DataHub ou OpenMetadata? | §6 |
| E se o volume fosse 10x? Cloud? | §7 |
| Lambda ou Kappa? | §8 + ADR 0001 |
| LGPD além do hash? | [`docs/security.md`](../security.md) (S3) |
| Como escalar horizontalmente? | §7 + diagrama de deployment |
| SCD2? Star schema? | [`data-model.md`](./data-model.md) |
