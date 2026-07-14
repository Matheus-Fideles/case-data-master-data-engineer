# Especificação do Smoke Test E2E

> **Audiência:** quem vai codar `tests/smoke/test_*.py`.
> **Status:** Spec narrativa. Lista o que validar; **não traz código pytest**.
> **Comando alvo:** `make smoke` localmente; job `smoke` no GitHub Actions.

## Filosofia

Smoke test **não substitui unit ou integration tests** — ele responde a uma pergunta única e crítica:

> *"O fluxo end-to-end está funcionando ou eu vou descobrir que quebrou na frente da banca?"*

Por isso:
- ✅ Cobre o **caminho feliz** com dados de fixture
- ✅ Verifica integração entre serviços (compose, MinIO, Postgres, Kafka, Spark, etc.)
- ✅ Valida invariantes críticas (PII não vaza, idempotência preserva resultado, SCD2 íntegro)
- ❌ **Não cobre edge cases** (responsabilidade dos unit tests)
- ❌ **Não cobre performance** (responsabilidade do load test)

## Targets de tempo

| Ambiente | Target | Razão |
|---|---|---|
| Local (`make smoke`) | < 3 min | Fluxo de feedback rápido para o engenheiro |
| CI GitHub Actions | < 5 min | Cobre overhead de boot do compose no runner |
| Pre-demo (`make smoke-full`) | < 10 min | Verificação completa antes da banca |

## Estrutura de arquivos

```
tests/
├── smoke/
│   ├── conftest.py                       # fixtures: compose up, MinIO clients, etc.
│   ├── test_01_infra_health.py
│   ├── test_02_bronze_ingestion.py
│   ├── test_03_silver_masking.py
│   ├── test_04_gold_scd2.py
│   ├── test_05_streaming_e2e.py
│   ├── test_06_idempotency.py
│   ├── test_07_quality_gates.py
│   └── test_08_security_invariants.py
└── fixtures/
    ├── sih_sus_sample_100.csv            # 100 linhas SIH-SUS pinadas
    ├── cnes_sample.json                  # 50 estabelecimentos
    ├── atendimentos_sample.jsonl         # 100 eventos para Kafka
    └── oltp_seed.sql                     # 100 pacientes determinísticos
```

A numeração nos nomes (`test_01_*`) controla a ordem de execução: infra primeiro, depois Bronze, depois Silver e assim por diante. Use `pytest-ordering` se quiser explicitar.

---

## Suite por arquivo — assertions detalhadas

### `test_01_infra_health.py` — Compose e dependências

| Assert | Como verificar |
|---|---|
| Compose subiu | `docker compose ps --format json` retorna ≥3 serviços `running` |
| Postgres-source aceita conexão | `psycopg.connect()` com timeout 10s |
| MinIO responde HEAD `/minio/health/live` | `requests.head` retorna 200 |
| Buckets criados | `boto3.list_buckets()` retorna `{bronze, silver, gold, landing}` |
| Buckets são privados | `mc anonymous get local/bronze` retorna `none` |
| Kafka aceita conexão | `confluent_kafka.AdminClient.list_topics()` retorna metadata |
| Tópico `atendimentos.raw` existe ou pode ser criado | `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` ou criação explícita |
| Postgres-DW aceita conexão (S2+) | idem source |
| Trino responde `SELECT 1` (S2+) | via `trino` driver |

**Tempo alvo:** 30s

---

### `test_02_bronze_ingestion.py` — Carga Bronze

Cobre o **caminho de extração e ingestão de uma fonte simples** (DataSUS amostra é a mais barata).

| Assert | Como verificar |
|---|---|
| Fixture `sih_sus_sample_100.csv` existe em `tests/fixtures/` | `pathlib.Path.exists()` |
| Função `pipelines.extraction.datasus_csv` aceita modo offline | Chamada com `offline_mode=True` retorna sucesso |
| Função `pipelines.batch.bronze_ingest_datasus` escreve Delta válido | Após chamada, `DeltaTable.forPath("s3a://bronze/sih_sus/...").detail()` não falha |
| Schema lido bate com `BronzeSIHSUSSchema` | Compara `df.schema` com `StructType` esperado |
| `count() == 100` | Linha conferida |
| Coluna de partição `(uf, ano_mes)` populada | `df.select("uf","ano_mes").distinct().count() == 1` |
| `_ingested_at` populada e recente | Compara com `now() - 5min` |
| `_source_file` é caminho da fixture | Substring match |
| `nk_aih` não tem nulos | `df.filter(col("nk_aih").isNull()).count() == 0` |
| `nk_aih` é único | `df.dropDuplicates(["nk_aih"]).count() == df.count()` |

**Tempo alvo:** 60s (inclui PySpark startup)

---

### `test_03_silver_masking.py` — Mascaramento ⚠️ **invariante crítica**

Cobre o caminho `bronze.oltp_paciente → silver.paciente_mascarado` e valida que **PII não vaza**.

| Assert | Como verificar |
|---|---|
| Fixture `oltp_seed.sql` populou Postgres com 100 pacientes determinísticos | `SELECT count(*) FROM oltp.paciente == 100` |
| `pipelines.batch.bronze_oltp_snapshot.run()` escreveu Bronze com PII | `bronze.oltp_paciente` tem 100 linhas, coluna `cpf` populada com 11 dígitos |
| `pipelines.batch.silver_paciente_mascaramento.run()` escreveu Silver | `silver.paciente_mascarado` tem 100 linhas |
| **`silver.cpf_hash` ≠ `bronze.cpf` para todas as linhas** | `EXCEPT` ou anti-join — qualquer linha em comum é regressão **CRÍTICA** |
| `silver.cpf_hash` é hash SHA-256 (64 chars hex) | regex match |
| **Mesmo CPF em Bronze → mesmo hash em Silver** | Determinismo: rodar duas vezes, hash é igual |
| `silver.nome` ≠ `bronze.nome` (Faker substituiu) | Compara linha-a-linha por `id_paciente` |
| `silver.email IS NULL` para todas as linhas | Supressão |
| `silver.telefone IS NULL` para todas as linhas | Supressão |
| `silver.cep` tem 3 dígitos (não 8) | `length(cep) == 3` |
| `silver.data_nascimento_ano` é INT entre 1900 e 2026 | Range check |

**Tempo alvo:** 30s

> Esse é o **assert mais importante do projeto**. Se quebrar, demo é cancelada.

---

### `test_04_gold_scd2.py` — Integridade SCD Tipo 2

Cobre `silver → gold.dim_paciente` (SCD2). Asserts derivados de [ADR 0006](../architecture/decisions/0006-scd2.md).

**Setup:** carregar Bronze inicial → Silver → Gold. Depois fazer UPDATE em 5 pacientes (mudança de CEP no Postgres). Recarregar.

| Assert | Como verificar |
|---|---|
| 1ª carga: `gold.dim_paciente` tem 100 linhas | count |
| 1ª carga: todas com `is_current = true` | `count(*) WHERE is_current=true == 100` |
| 1ª carga: todas com `valid_to ≥ '9999-01-01'` | range check |
| Após 5 UPDATEs no OLTP + 2ª carga: `gold.dim_paciente` tem 105 linhas | 100 originais + 5 novas versões |
| Após 2ª carga: ainda 100 linhas com `is_current = true` | invariante |
| Não há overlap temporal | `SELECT nk, valid_from, valid_to GROUP BY nk HAVING ...` — query de overlap detection |
| `valid_to` da versão antiga = `valid_from` da nova (sem gap) | join entre versões consecutivas |
| Linhas atualizadas têm dois `_attr_hash` diferentes | sanity |
| Linhas não atualizadas têm `is_current=true` único | sanity |

**Tempo alvo:** 60s

---

### `test_05_streaming_e2e.py` — Producer → Kafka → Spark → Bronze (S3)

| Assert | Como verificar |
|---|---|
| Producer publica 100 eventos da fixture em `atendimentos.raw` | `confluent_kafka.Producer.produce()` em loop, `flush()` |
| Spark consumer lê e escreve em `bronze.atendimentos_stream` em ≤ 90s | Polling com timeout |
| Linhas em Bronze == 100 | count |
| Schema lido bate com `BronzeAtendimentosStreamSchema` | StructType compare |
| `id_paciente` em todos os eventos existe em `oltp.paciente` | LEFT ANTI JOIN deve ser empty |
| Watermark funcionou: evento da fixture com `ts_evento` 2 horas no passado **não está em Bronze** | LATE_EVENT virou DLQ |
| `atendimentos.dlq` tem entrada com `error_reason='LATE_EVENT'` | consumer DLQ |
| Idempotência: republicar 1 evento já recebido **não duplica** Bronze | re-publicar + count Bronze == 100 |

**Tempo alvo:** 120s (streaming inclui watermark assert)

---

### `test_06_idempotency.py` — Reexecução não corrompe

| Assert | Como verificar |
|---|---|
| Rodar `dag_bronze_datasus_sih` duas vezes para SP/2024-01 | mesmo `count()` e `_row_hash` igual |
| Rodar `dag_silver_paciente_mascaramento` duas vezes | `count()` e `_attr_hash` igual; `_loaded_at` da segunda execução == primeira (NO-OP detection) |
| Rodar `dag_gold_fato_internacao` duas vezes | MERGE NO-OP — confirmado por `merge_inserted_count == 0, merge_updated_count == 0` no log |
| Métrica de pipeline (`pipeline_records_written_total`) bate entre execuções | sanity |

**Tempo alvo:** 60s

---

### `test_07_quality_gates.py` — Great Expectations (S4)

| Assert | Como verificar |
|---|---|
| Suite Bronze SIH passa (`expect_column_to_exist nk_aih`, etc.) | `ge.run_checkpoint("bronze_sih_checkpoint")` retorna success |
| Suite Silver paciente passa | idem |
| Suite Gold integridade referencial passa | todos os `sk_*` em fato resolvem para dim |
| Quebra intencional (corromper dado) é detectada | injetar nulo em NK, rodar suite, esperar falha |

**Tempo alvo:** 30s

---

### `test_08_security_invariants.py` — Política de segurança

| Assert | Como verificar |
|---|---|
| Buckets MinIO não têm acesso anônimo | `mc anonymous get` retorna `none` para os 4 |
| Postgres-source role `app_writer` não pode SELECT em pg_authid | tenta query, espera erro permissão |
| Trino RBAC: role `analyst` não acessa `lake_bronze` | tenta SELECT, espera erro |
| `.env` está no `.gitignore` | grep no .gitignore |
| `PII_HASH_SALT` tem ≥ 32 chars | env check |
| Não há credenciais hardcoded em arquivos versionados | `git grep -E '(password\|secret\|api_key)\s*=' --` retorna apenas `.env.example` |
| **Nenhum CPF original aparece em `silver.*` ou `gold.*`** | extra-paranoia: scan completo dos buckets |

**Tempo alvo:** 30s

---

## Suite mínima vs completa

| Suite | Quando rodar | Arquivos |
|---|---|---|
| **Mínima** (`make smoke-min`) | Pre-commit, push para branch dev | 01, 02, 03, 06, 08 |
| **Completa** (`make smoke`) | PR para main, CI | 01–08 |
| **Pre-demo** (`make smoke-full`) | Antes da banca | tudo + load test leve (1000 eventos streaming) |

## Fixtures determinísticas

- Mesmo seed em Faker (`Faker.seed(42)`)
- Mesmo conteúdo em CSVs (commitados via LFS)
- Mesmo conteúdo em fixtures Kafka (JSONL)
- Hashes SHA-256 dos fixtures verificados em conftest (proteção contra corrupção)

## Conftest essencial

`tests/smoke/conftest.py` deve prover (de forma narrativa, não código):

- Fixture `compose_up` (scope=session) que sobe `make up-s1` no setup, derruba no teardown — pular se variável `KEEP_COMPOSE=1` (acelera dev local)
- Fixture `minio_client` (scope=module) que retorna `boto3` configurado para MinIO local
- Fixture `pg_source` que retorna `psycopg.Connection` para Postgres-source
- Fixture `seed_oltp` (scope=session) que popula Postgres com `tests/fixtures/oltp_seed.sql` se vazio
- Fixture `kafka_producer` que retorna producer configurado
- Fixture `spark` (scope=session) — SparkSession local com configs Delta+S3A
- Helper `wait_for(condition, timeout)` para esperar healthchecks

## CI — workflow GitHub Actions (alto nível)

```
.github/workflows/ci.yml
  jobs:
    lint:
      - ruff check .
      - black --check .
      - yamllint -d relaxed .
    unit:
      - needs: lint
      - pytest tests/unit -v --timeout=60
    smoke:
      - needs: unit
      - docker compose --profile core --profile streaming up -d
      - make wait-healthy
      - make seed
      - pytest tests/smoke -v --timeout=180
      - make down
    
    concurrency:
      group: ${{ github.ref }}
      cancel-in-progress: true
```

Cache:
- `actions/cache` em `~/.cache/pip` e `~/.cache/uv`
- `docker/build-push-action` com cache de imagens
- LFS pull antes do compose up

## Defesa em banca

| Pergunta | Resposta de 30s |
|---|---|
| Qual a sua estratégia de testes? | Pirâmide: muitos unit (transformações isoladas), alguns integration (Spark+MinIO), poucos smoke (E2E). Smoke é o que roda em CI a cada PR e antes da demo. |
| Como vocês garantem que PII não vaza? | Smoke test 03 faz EXCEPT entre Bronze.cpf e Silver.cpf_hash. Qualquer overlap é falha crítica que bloqueia merge. |
| O que acontece se o smoke test passar mas a demo falhar? | Smoke não cobre edge cases nem load. Pre-demo (`make smoke-full`) adiciona load test de 1000 eventos streaming. |
| Como vocês evitam testes flaky em CI? | Fixtures determinísticas, timeouts generosos com causa (não número mágico), `concurrency.cancel-in-progress` evita sobreposição, retry de 1 em flake conhecido. |
| O smoke test sobe o compose ou usa serviços já rodando? | Em CI sempre sobe. Em dev local, `KEEP_COMPOSE=1` permite reutilizar para iteração rápida. |

## Referências

- **pytest-ordering:** <https://pypi.org/project/pytest-ordering/>
- **pytest-docker:** <https://pypi.org/project/pytest-docker/> (alternativa ao compose-from-conftest manual)
- **Great Expectations:** <https://docs.greatexpectations.io/>
- **Testcontainers Python:** <https://testcontainers-python.readthedocs.io/> (alternativa moderna)
- **ADRs relevantes:** [0002 idempotência](../architecture/decisions/0002-idempotency-merge.md), [0004 mascaramento](../architecture/decisions/0004-pii-masking.md), [0006 SCD2](../architecture/decisions/0006-scd2.md)
