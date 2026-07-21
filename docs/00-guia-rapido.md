# Guia de Reprodução — Case Santander Data Master

> Instruções completas para os avaliadores reproduzirem a plataforma do zero, em ordem.

**Repositório:** https://github.com/Matheus-Fideles/case-data-master-data-engineer  
**Stack:** Airflow · Spark on k3s · Delta Lake · MinIO · Trino · Metabase · Prometheus · Grafana · Marquez

---

## Pré-requisitos

| Requisito | Versão mínima | Verificação |
|---|---|---|
| Docker Desktop / Rancher Desktop | 4.x / 1.x | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| kubectl | 1.28+ | `kubectl version --client` |
| Python | 3.10+ | `python3 --version` |
| make | any | `make --version` |

> **Rancher Desktop** é recomendado pois já inclui k3s (Kubernetes local) necessário para o Spark.  
> Configure: *Preferences → Kubernetes → Enable Kubernetes*.

---

## 1. Clone e configuração inicial

```bash
git clone https://github.com/Matheus-Fideles/case-data-master-data-engineer.git
cd case-data-master-data-engineer

# Copiar variáveis de ambiente
cp .env.example .env
```

O `.env.example` contém valores seguros para ambiente de desenvolvimento local.  
**Não é necessário alterar nada** para a demonstração funcionar.

---

## 2. Subir o ambiente

```bash
# Core: Postgres + MinIO + Airflow (obrigatório)
make up-core

# Observabilidade: Prometheus + Grafana + Marquez + cAdvisor (opcional)
docker compose --profile observability up -d

# Serving: Hive Metastore + Trino + Metabase (opcional — BI)
docker compose --profile serving up -d

# Streaming: Kafka + Kafka UI + stream-producer (opcional)
docker compose --profile streaming up -d
```

**Atalho para tudo de uma vez:**

```bash
make up-all
# equivale a: docker compose --profile core --profile observability --profile serving --profile streaming up -d
```

### Aguardar os healthchecks

```bash
docker compose ps
# Todos os serviços devem mostrar "(healthy)" antes de prosseguir
# Trino e Metabase levam ~2 minutos para inicializar
```

---

## 3. Aplicar policies IAM no MinIO

O `minio-init` cria as policies automaticamente quando sobe com o profile `core`.  
Se precisar reaplicar manualmente:

```bash
docker run --rm --network host --entrypoint /bin/sh minio/mc:latest -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc admin policy create local pipeline-rw infra/minio/init/policy-pipeline.json
  mc admin policy create local serving-ro  infra/minio/init/policy-serving.json
  mc admin user add local svc-pipeline pipeline-secret-change-me
  mc admin policy attach local pipeline-rw --user svc-pipeline
  mc admin user add local svc-serving serving-secret-change-me
  mc admin policy attach local serving-ro  --user svc-serving
"
```

**Modelo de acesso (least-privilege):**

| Usuário | Policy | Acesso |
|---|---|---|
| `root` (minioadmin) | admin | Administração do cluster MinIO |
| `svc-pipeline` | `pipeline-rw` | Leitura/escrita em landing, bronze, silver, gold |
| `svc-serving` | `serving-ro` | **Somente leitura** em gold (Trino/Metabase) |

---

## 4. Aplicar secrets no Kubernetes

```bash
make k8s-secrets
# equivale a: envsubst < k8s/minio-secret.yaml | kubectl apply -f -
#             envsubst < k8s/pii-secret.yaml    | kubectl apply -f -
```

---

## 5. Executar o pipeline

### Via Airflow UI (http://localhost:8080)

Login: `admin` / `admin`

Ordem recomendada de execução:

1. **Bronze** — trigger manual em qualquer DAG `dag_bronze_*`  
   Exemplo: `dag_bronze_sim_obitos` com config: `{"data_ref": "2024-01-01"}`
2. **Silver** — trigger `dag_silver_sim_obitos` após o Bronze completar
3. **Gold** — trigger `dag_gold_fato_obito` após o Silver completar

> Com `OFFLINE_MODE=1` no `.env` os jobs usam dados cacheados em `data/raw/` e não precisam de conexão com o DataSUS.

### Via Makefile (modo rápido)

```bash
make run-bronze  # submete todos os jobs Bronze
make run-silver  # submete todos os jobs Silver
make run-gold    # submete todos os jobs Gold
```

---

## 6. Verificar o pipeline (testes)

```bash
# Instalar dependências de teste
pip install -r requirements-dev.txt

# 106 testes unitários
python -m pytest tests/extraction/ tests/batch/ -v

# 36 testes de fumaça (requer os containers up)
python -m pytest tests/smoke/ -v

# Todos de uma vez
make test
```

---

## 7. Acessar as UIs

| Interface | URL | Credenciais |
|---|---|---|
| **Airflow** (orquestração) | http://localhost:8080 | admin / admin |
| **MinIO** (data lake) | http://localhost:9001 | minioadmin / minioadmin |
| **Kafka UI** (streaming) | http://localhost:8090 | — |
| **Hive Metastore** | thrift://localhost:9083 | — (sem UI web) |
| **Trino** (query engine) | http://localhost:8085 | — (sem senha) |
| **Metabase** (BI) | http://localhost:3001 | matheuss.fideles@hotmail.com / Admin1234! |
| **Grafana** (métricas) | http://localhost:3000 | admin / admin |
| **Prometheus** (scraping) | http://localhost:9090 | — |
| **cAdvisor** (containers) | http://localhost:8088 | — |
| **Marquez Web** (lineage) | http://localhost:5012 | — |
| **Portal de Serviços** | docs/portal.html | — (abrir no browser) |

---

## 8. Consultar dados no Trino

```bash
# CLI do Trino (dentro do container)
docker exec -it case-data-master-data-engineer-trino-1 trino

# Exemplos de consulta
SHOW SCHEMAS FROM delta;
SHOW TABLES FROM delta.gold;
SELECT * FROM delta.gold.fato_obito LIMIT 10;
SELECT municipio, SUM(total_obitos) FROM delta.gold.kpi_mortalidade GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

---

## 9. Verificar observabilidade

**Prometheus targets** (http://localhost:9090/targets):
- `prometheus` → UP
- `minio` → UP
- `cadvisor` → UP

**Grafana Dashboard** (http://localhost:3000 → Dashboard → "Case Santander — Pipeline Overview"):
- Status dos serviços, CPU/memória por container, métricas MinIO

**Marquez Lineage** (http://localhost:5012):
- Namespaces: `case-santander` e `batch`
- Grafo de linhagem dos jobs Spark após execução do pipeline

---

## 10. Estrutura do repositório

```
.
├── airflow/dags/          # 17 DAGs (bronze/, silver/, gold/, quality/)
├── apps/                  # Jobs PySpark (batch/ + streaming/)
├── apps/shared/           # Utilitários compartilhados (masking, spark_utils)
├── data/raw/              # Dados cacheados para OFFLINE_MODE=1
├── docs/                  # Documentação técnica + portal.html + resumo-executivo.pdf
├── infra/
│   ├── grafana/           # Dashboard JSON + provisioning
│   ├── minio/init/        # Script de criação de buckets + policies IAM
│   ├── postgres/          # Init SQL (schemas, roles)
│   ├── prometheus/        # prometheus.yml + alerting rules
│   └── trino/             # Catalog delta.properties + config.properties
├── k8s/
│   ├── minio-secret.yaml  # Template do Secret MinIO (svc-pipeline)
│   ├── pii-secret.yaml    # Template do Secret PII_SALT
│   └── sparkapplications/ # SparkApplication CRDs (um por job)
├── tests/
│   ├── batch/             # Testes unitários dos jobs Spark
│   ├── extraction/        # Testes dos extractors
│   └── smoke/             # Testes de fumaça de infraestrutura
├── docker-compose.yml     # 4 profiles: core | streaming | serving | observability
├── Makefile               # Atalhos principais
└── .env.example           # Variáveis de ambiente com valores seguros para demo
```

---

## 11. Troubleshooting

### Spark job falha com ExitCode 1

```bash
# Ver logs do driver pod
kubectl logs -n spark -l spark-role=driver --tail=100

# Problema comum: data malformada do DataSUS
# Solução já aplicada: spark.sql.legacy.timeParserPolicy: CORRECTED no SparkApplication YAML
```

### Trino não conecta ao MinIO

```bash
# Verificar credenciais svc-serving no MinIO
docker run --rm --network host --entrypoint /bin/sh minio/mc:latest -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc admin user list local
"

# Testar acesso direto
docker exec case-data-master-data-engineer-trino-1 \
  curl -sf http://minio:9000/minio/health/ready
```

### Metabase não conecta ao Trino

```bash
# Verificar se Trino aceita X-Presto-User (compatibilidade Presto JDBC)
curl -s -X POST http://localhost:8085/v1/statement \
  -H "X-Presto-User: trino" \
  -d "SELECT 1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('stats',{}).get('state'))"
# Deve retornar: QUEUED

# A propriedade protocol.v1.alternate-header-name=Presto deve estar em infra/trino/etc/config.properties
```

### Prometheus sem targets

```bash
# Verificar se cAdvisor está UP
curl -sf http://localhost:8088/metrics | head -5

# Verificar MinIO metrics
curl -sf http://localhost:9000/minio/v2/metrics/cluster | head -5
```
