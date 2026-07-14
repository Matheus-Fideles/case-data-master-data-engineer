# Escopo Técnico — Componentes da Solução

- **Status:** Aceito (revisado 2026-07-13)
- **Data:** 2026-05-07 · **Última revisão:** 2026-07-13
- **Objetivo:** definir quais componentes são obrigatórios, quais são "nice-to-have" e quais ficam em "Evolução Futura". Decidir antes de codar evita inchar o Compose, estourar RAM no laptop e atrasar a entrega.

## Princípio orientador

A banca avalia **corretude técnica + clareza + reprodutibilidade**, não quantidade de logos no diagrama. Cada serviço extra cobra um custo: RAM, tempo de boot, configuração, debug, e mais uma coisa que pode quebrar ao vivo na demo de 1h30.

Critério de inclusão: **um serviço entra como obrigatório se cobre um requisito explícito do enunciado**. Caso contrário, vai para evolução futura.

## Componentes — três níveis

### 🟢 Núcleo obrigatório — Docker Compose

| Componente | Cobre requisito | Justificativa |
|---|---|---|
| **Postgres** (1 instância, 4 schemas) | Extração + Armazenamento | `oltp` = source Faker PII · `airflow` = backend · `marquez` = lineage · `gold_dw` = DW dimensional |
| **MinIO** | Armazenamento + Arquitetura | Data lake S3-compatível; buckets `landing/bronze/silver/gold` |
| **Kafka** (KRaft, sem Zookeeper) | Ingestão | Backbone de streaming; tópico `notificacoes.raw` |
| **Airflow** | Ingestão + Arquitetura | Orquestração batch; `PythonOperator` para extração, `SparkKubernetesOperator` para transformação |
| **stream-producer** (container Python) | Ingestão streaming | Faker → Kafka; separado do Airflow por ser processo contínuo |
| **Trino** | Armazenamento + Arquitetura | Query federada lake↔DW + RBAC por catálogo |
| **Prometheus** | Observabilidade | Coleta de métricas de todos os serviços |
| **Grafana** | Observabilidade | Dashboards provisionados automaticamente |
| **Marquez** (+OpenLineage) | Observabilidade | Lineage de dados — "rastrear fluxo" (requisito 4) |
| **Metabase** | Análise | Visualização end-to-end para demo |

### 🟢 Núcleo obrigatório — Rancher Desktop (k3s)

> **Pré-requisito do avaliador:** Rancher Desktop instalado e rodando. Ver [ADR 0007](./decisions/0007-spark-on-kubernetes.md).

| Componente | Cobre requisito | Justificativa |
|---|---|---|
| **spark-on-k8s-operator** (Helm) | Ingestão + Escalabilidade + Arquitetura | Gerencia SparkApplication CRDs no k3s local |
| **Spark driver + executors** (pods) | Ingestão + Escalabilidade | Processamento distribuído Bronze→Silver→Gold + Structured Streaming |

**Spark não entra no Docker Compose.** Decisão formal em [ADR 0007](./decisions/0007-spark-on-kubernetes.md).

**Bibliotecas embarcadas (não são serviços):**
- **Great Expectations** — validações de qualidade nos DAGs
- **Faker** — geração de dados sintéticos para stream e mascaramento de nomes
- **dbt-core + dbt-trino** — transformações Silver→Gold (opcional, decisão final na S2)

**Total:** 10 serviços no Compose. Roda em laptop com 16 GB de RAM usando profiles.

### 🟡 Sob avaliação (decisão final na S2)

| Componente | Por que pode entrar | Por que pode sair |
|---|---|---|
| **dbt-trino** | Modelagem declarativa, lineage automático, testes integrados | Spark SQL puro cobre tudo; menos uma dependência |
| **Debezium** | CDC real do Postgres OLTP — defesa forte para "ingestão contínua" | Pode ser substituído por polling Airflow no Postgres |

**Critério para decidir:** se a S1 entrega Bronze→Silver→Gold básico até dia 13/05 com folga, dbt entra na S2. Caso contrário, fica em evolução futura.

### 🔴 Movidos para "Evolução Futura" (saem do Compose, entram no slide final)

| Componente | Por que está fora | Como aparece no entregável |
|---|---|---|
| **DataHub** | Pesado (vários containers Java), instável em Compose, e o requisito de catálogo é parcialmente coberto pelo Marquez (lineage) | Screenshots da UI (rodar em separado, capturar) + seção dedicada no README "Governança e Catálogo" |
| **Loki** | Logs do Docker (`docker logs`) e do Airflow já cobrem o caso da banca; Loki adiciona um painel sem cobrir requisito novo | Mencionado no slide de evolução; mostrado como "próximo passo de observabilidade" |
| **Vault** | Configurar TLS + sealing + unsealing em laptop é caro pelo benefício; `.env` + Docker secrets cobre criptografia em repouso de credenciais | Diagrama de "arquitetura alvo cloud" mostra Vault/AWS Secrets Manager; explicar trade-off no README |
| **Apache Ranger / OPA** | RBAC é coberto por Trino + Postgres roles | Mencionar como evolução para autorização fina por coluna |
| **Iceberg / Hudi** | Decidimos por Delta no ADR — mostrar comparativo, não rodar três | Aparece na matriz comparativa |

## Profiles do Docker Compose

| Profile | Serviços |
|---|---|
| `core` | postgres (1 instância), minio, minio-init, airflow |
| `streaming` | kafka, stream-producer |
| `serving` | trino, metabase |
| `observability` | prometheus, grafana, marquez |

```bash
make up-core          # postgres + minio + airflow (sem streaming)
make up-all           # todos os profiles Compose → demo final
make k8s-setup        # provisiona namespace spark + spark-operator no Rancher Desktop
make k8s-spark-image  # build + push imagem Spark custom para registry local
make smoke            # testes E2E completos
make warmup           # docker compose pull + k8s pull — 10 min antes da banca
```

## Decisão sobre fontes de dados externas (APIs)

**Mudança de 2026-07-13:** fontes migradas de CSV/FTP DataSUS para REST API pública do Ministério da Saúde (`apidadosabertos.saude.gov.br`). Não requer autenticação.

| Fonte | Endpoint | Estratégia para demo |
|---|---|---|
| CNES estabelecimentos | `/cnes/estabelecimentos` | Cache JSON em `data/raw/cnes/` |
| Dengue notificações | `/arboviroses/dengue?nu_ano=2024` | Cache JSON em `data/raw/dengue/` |
| Zika notificações | `/arboviroses/zikavirus?nu_ano=2024` | Cache JSON em `data/raw/zika/` |
| Chikungunya notificações | `/arboviroses/chikungunya?nu_ano=2024` | Cache JSON em `data/raw/chikungunya/` |
| Óbitos (SIM) | `/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade` | Cache JSON em `data/raw/sim/` |
| Vacinação PNI | `/vacinacao/doses-aplicadas-pni-2024` | Cache JSON em `data/raw/vacinacao/` |
| Municípios + regiões | `/macrorregiao-e-regiao-de-saude/municipio` | Cache JSON em `data/raw/municipios/` |
| OLTP pacientes | Postgres (`oltp` schema) | Gerado por `make seed` (Faker) |
| Streaming | Kafka `notificacoes.raw` | Gerado por `stream-producer` container |

**Razão do cache:** depender de internet no dia da demo é risco. `make seed` baixa e armazena amostras em `data/raw/` na primeira execução. Demo subsequentes usam cache offline.

## Resumo numérico (revisado 2026-07-13)

| Dimensão | Valor |
|---|---|
| Serviços Docker Compose | 10 (sem Spark) |
| Pods k8s (Rancher Desktop) | driver + 2 executors por job Spark |
| RAM Compose | ~8 GB |
| RAM Rancher Desktop | ≥ 8 GB alocados |
| Boot completo (`make up-all` + k8s healthy) | ~5 min |
| 5 pontos de falha externa | 0 pontos críticos (tudo cacheável) |

## Próxima revisão

Reavaliar este documento ao final da S2 (20/05). Se sobrar tempo, considerar reincluir Loki ou DataHub. Se faltar, considerar cortar dbt e/ou Trino.
