# Governança de Dados e Compliance LGPD

> Cobre segurança, privacidade, mascaramento, controle de acesso, dicionário de dados e padrões de qualidade de código.

---

## Sumário

- [1. Classificação dos Dados](#1-classificação-dos-dados)
- [2. LGPD — Base Legal e Fluxo de Mascaramento](#2-lgpd--base-legal-e-fluxo-de-mascaramento)
- [3. Mascaramento Técnico (ADR-004)](#3-mascaramento-técnico-adr-004)
- [4. Controle de Acesso (RBAC)](#4-controle-de-acesso-rbac)
- [5. Gestão de Segredos](#5-gestão-de-segredos)
- [6. Modelo de Ameaças](#6-modelo-de-ameaças)
- [7. Auditoria e Lineage](#7-auditoria-e-lineage)
- [8. Dicionário de Dados — Camada Gold](#8-dicionário-de-dados--camada-gold)
- [9. Padrões de Engenharia — Pre-commit Hooks](#9-padrões-de-engenharia--pre-commit-hooks)
- [10. FAQ Técnico](#10. FAQ Técnico)

---

## 1. Classificação dos Dados

| Classificação | Exemplos | Camadas que contêm |
|---|---|---|
| **PII Direto** | CPF, nome, data de nascimento, e-mail, telefone, CEP completo | Bronze apenas (oltp/paciente) |
| **PII Derivado** | CEP-3 dígitos, ano de nascimento | Silver, Gold |
| **Pseudonimizado** | `id_paciente_hash` (SHA-256+salt) | Silver, Gold |
| **Público** | Dados epidemiológicos, municípios, vacinas | Bronze, Silver, Gold |

**Regra fundamental:** após o Silver, **nenhum dado PII direto pode existir** em nenhuma camada. `validate_no_pii()` é chamado obrigatoriamente no final de cada job Silver que toca dados de pacientes.

---

## 2. LGPD — Base Legal e Fluxo de Mascaramento

**Base legal:** tutela da saúde pública (Art. 7º, VIII) + dados anonimizados não são dados pessoais (Art. 12).

**Fluxo de ciclo de vida do PII:**

```
Postgres OLTP (PII real — Faker sintético)
    │
    ▼  [apps/oltp/jobs/extract_oltp_snapshot.py]
s3a://bronze/oltp/paciente/  ← ÚNICO local com PII
    │  Retenção: 7 dias após mascaramento Silver
    │  Acesso restrito: apenas job Silver pode ler
    │
    ▼  [apps/epidemiologico/jobs/silver_paciente.py]
    │  mask_paciente(df, cpf_col="cpf", output_col="id_paciente_hash")
    │  validate_no_pii(df)  ← bloqueia se PII sobreviver
    │
s3a://silver/oltp/paciente/  ← SEM PII direto
    │
    ▼
s3a://gold/*/  e  Postgres gold_dw  ← SEM PII direto
```

**Direito ao Esquecimento:** implementado via `DELETE` + `VACUUM` no Delta Lake com retenção de 7 dias.

---

## 3. Mascaramento Técnico (ADR-004)

Implementado em `apps/shared/masking.py`:

| Campo PII | Técnica | Output |
|---|---|---|
| `cpf` | SHA-256 + `PII_SALT` (HMAC-like) | `id_paciente_hash` (64 hex chars) |
| `nome_completo` | Suprimido (drop) | — |
| `data_nascimento` | Truncado para ano | `ano_nascimento` |
| `email` | Suprimido (drop) | — |
| `telefone` | Suprimido (drop) | — |
| `cep` | Primeiros 3 dígitos | `cep_regiao` |

**Propriedades do hash:**
- **Determinístico:** mesmo CPF + mesmo salt → mesmo hash (permite joins entre execuções)
- **Irreversível:** SHA-256 one-way; sem o salt, não há reversão
- **Consistente:** `id_paciente_hash` no Kafka (stream-producer) usa o mesmo algoritmo, permitindo joins stream × batch

**`PII_SALT`** armazenado em k8s Secret (`k8s/postgres-secret.yaml`). **Nunca hardcoded** em código ou `.env`.

```python
# apps/shared/masking.py — contrato público
def mask_paciente(df: DataFrame, *, cpf_col: str, output_col: str) -> DataFrame:
    ...

def validate_no_pii(df: DataFrame, pii_cols: list[str] | None = None) -> None:
    """Raises ValueError se qualquer coluna PII sobreviver."""
    ...
```

**Hook anti-PII no pre-commit** verifica que nenhum arquivo Python referencia `cpf`, `rg`, `nome_completo`, `data_nascimento`, `telefone` ou `email_pessoal` fora dos únicos arquivos autorizados:
- `apps/oltp/jobs/extract_oltp_snapshot.py`
- `apps/shared/masking.py`
- `tests/`

---

## 4. Controle de Acesso (RBAC)

### Postgres — Roles

| Role | Acesso | Usuário |
|---|---|---|
| `gold_engineer` | Leitura/Escrita em `gold_dw` | Jobs Spark (psycopg2) |
| `gold_analyst` | Apenas leitura em `gold_dw` | Trino, consultas ad-hoc |
| `airflow` | Schema `airflow` (backend) | Airflow scheduler/webserver |
| `oltp_app` | Schema `oltp` (leitura) | Job de extração snapshot |

### Trino — Catálogos

| Catálogo | Acesso | Dados |
|---|---|---|
| `delta` | Leitura (analistas) | Gold Delta Lake via HMS |
| `hive` | Interno (metastore) | Catalog metadata |

RBAC granular por coluna (ex.: mascarar CPF para analistas externos) é evolução futura via Apache Ranger.

### MinIO — IAM Policies (Least-Privilege)

O MinIO implementa controle de acesso baseado em políticas IAM compatíveis com AWS S3.
Dois service accounts são criados automaticamente pelo `minio-init` na inicialização:

| Usuário | Policy | Permissões | Usado por |
|---|---|---|---|
| `svc-pipeline` | `pipeline-rw` | `s3:GetObject`, `PutObject`, `DeleteObject`, `ListBucket` em todos os buckets | Spark jobs, Airflow |
| `svc-serving` | `serving-ro` | `s3:GetObject`, `ListBucket` **somente em `gold`** | Trino, Metabase |

**Nenhum serviço analítico tem acesso de escrita** — o Trino (e portanto o Metabase) só consegue ler o bucket `gold` via `svc-serving`, não podendo deletar ou sobrescrever dados.

O usuário `minioadmin` (root) é usado apenas para administração do cluster MinIO e não é injetado em nenhum serviço de dados.

**Políticas em JSON** — `infra/minio/init/create-buckets.sh`:

```json
// pipeline-rw
{ "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],
  "Resource": ["arn:aws:s3:::landing/*","arn:aws:s3:::bronze/*","arn:aws:s3:::silver/*","arn:aws:s3:::gold/*"] }

// serving-ro
{ "Action": ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
  "Resource": ["arn:aws:s3:::gold", "arn:aws:s3:::gold/*"] }
```

---

## 5. Gestão de Segredos

### Princípio: `.env` como fonte única de verdade

**Nenhuma credencial** é hardcoded em arquivos de configuração, YAML ou XML. O `.env` é o único ponto onde valores concretos existem — e nunca é commitado.

```
.env  (gitignored)  →  Docker Compose (env_file: .env)
                    →  hive-site.xml  via ${env.VAR}     ← Hadoop Configuration nativo
                    →  delta.properties via ${ENV:VAR}   ← Trino 348+ nativo
                    →  k8s/secrets via make k8s-secrets  ← envsubst antes de kubectl apply
```

| Segredo | Storage local | Storage produção | Mecanismo de injeção |
|---|---|---|---|
| `PII_SALT` | `.env` → `make k8s-secrets` | AWS Secrets Manager + ESO | k8s Secret → env var no pod |
| Credenciais Postgres | `.env` | AWS Secrets Manager | Env var / k8s Secret |
| Credenciais MinIO (S3A) | `.env` | AWS IAM Role (IRSA) | `${env.VAR}` no hive-site.xml · `${ENV:VAR}` no delta.properties |
| Credenciais Kafka | `.env` | AWS MSK IAM auth | Env var |
| Hive Metastore DB | `.env` (`POSTGRES_HIVE_*`) | RDS IAM auth | `${env.VAR}` no hive-site.xml |

**Regras:**
- `.env` **nunca commitado** (`.gitignore`); `.env.example` tem apenas valores de demo
- Arquivos `k8s/*.yaml` são **templates** com `${VAR}` — `make k8s-secrets` faz o `envsubst` e aplica
- `detect-secrets` no pre-commit bloqueia qualquer segredo acidental em commit
- Em produção: External Secrets Operator sincroniza AWS Secrets Manager → k8s Secrets automaticamente

---

## 6. Modelo de Ameaças

| Ameaça | Mitigação |
|---|---|
| PII vaza para Silver/Gold | `validate_no_pii()` obrigatório + hook anti-PII |
| Credencial hardcoded no código | `detect-secrets` pre-commit + CI |
| Reprocessamento corrompe dados | MERGE idempotente por chave natural (ADR-002) |
| Schema da API muda sem aviso | `mergeSchema=true` Bronze + validação Silver (ADR-003) |
| Acesso não autorizado ao bronze | MinIO bucket policy restrita a service accounts |
| Evento streaming com PII | Producer Faker já envia `id_paciente_hash` — sem PII no Kafka |

---

## 7. Auditoria e Lineage

**OpenLineage + Marquez:** cada DAG Airflow emite eventos de lineage via `emit_lineage` task final.

```
DAG bronze_dengue
  extract → ingest_spark → quality_gate → emit_lineage
                                               │
                                               ▼
                                        Marquez (localhost:5000)
                                        lineage: REST API → bronze/epidemiologico/dengue/
```

**Delta Lake time travel:** auditoria de quando qualquer dado foi escrito/alterado.

```sql
-- Quem escreveu esta partição e quando?
DESCRIBE HISTORY delta.`s3a://bronze/oltp/paciente/`;

-- Restaurar versão anterior após erro
RESTORE delta.`s3a://gold/epidemiologico/fatos_notificacao/` TO VERSION AS OF 3;
```

**`_batch_id`** em todas as tabelas rastreia qual execução Airflow produziu cada linha.

---

## 8. Dicionário de Dados — Camada Gold

### Convenções

- **SK (Surrogate Key):** chave artificial gerada Silver→Gold. Tipo `BIGINT`. Prefixo `sk_`.
- **NK (Natural Key):** chave do mundo real. Tipo `STRING`. Prefixo `nk_`.
- **SCD2:** colunas `valid_from DATE`, `valid_to DATE`, `is_current BOOLEAN`, `_attr_hash VARCHAR`.
- **Métrica aditiva:** `SUM()` faz sentido em todas as dimensões.
- **Métrica semi-aditiva:** `SUM()` faz sentido apenas em algumas dimensões (ex.: posição geográfica).

### `fato_notificacao` (Gold Delta — epidemiologico)

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_notificacao` | BIGINT | Surrogate key |
| `nk_id_notificacao` | STRING | Hash composto como chave idempotência |
| `sk_agravo` | BIGINT | FK → `dim_agravo` |
| `sk_municipio_notificacao` | BIGINT | FK → `dim_municipio` |
| `sk_municipio_residencia` | BIGINT | FK → `dim_municipio` |
| `sk_tempo_notificacao` | BIGINT | FK → `dim_tempo` |
| `cs_sexo` | STRING | M/F/I |
| `nu_idade_anos` | INT | Idade calculada (Silver decodifica `nu_idade_n`) |
| `classi_fin` | STRING | Classificação final do caso |
| `semana_epidemiologica` | INT | Semana epidemiológica |
| `ano_mes` | STRING | Partição (`YYYYMM`) |
| `_batch_id` | STRING | Rastreabilidade da carga |
| `_load_ts` | TIMESTAMP | Quando foi carregado |

### `fato_atendimento_stream` (Gold Delta — streaming)

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_atendimento` | BIGINT | Gerado por monotonically_increasing_id |
| `sk_paciente` | BIGINT | FK → `dim_paciente` (Postgres) |
| `sk_estabelecimento` | BIGINT | FK → `dim_estabelecimento` (Postgres) |
| `ts_evento` | TIMESTAMP | Event time (fonte: Kafka) |
| `tipo_atendimento` | STRING | CONSULTA / URGENCIA / INTERNACAO |
| `triagem` | STRING | Triagem Manchester |
| `_kafka_offset` | LONG | Offset Kafka para rastreabilidade |
| `_kafka_partition` | INT | Partição Kafka |
| `ano_mes` | STRING | Partição Delta |
| `_batch_id` | STRING | Número do microbatch Spark |
| `_load_ts` | TIMESTAMP | Processing time |

### `dim_paciente` (Postgres `gold_dw` — SCD2)

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_paciente` | SERIAL PK | Surrogate key (autoincremental) |
| `id_paciente_hash` | VARCHAR(64) | SHA-256+salt do CPF — chave de negócio |
| `sexo` | CHAR(1) | M/F |
| `ano_nascimento` | SMALLINT | Ano de nascimento (sem dia/mês — LGPD) |
| `cep_regiao` | CHAR(3) | Primeiros 3 dígitos do CEP |
| `municipio_codigo_ibge` | VARCHAR(7) | Município de residência |
| `dt_inicio` | DATE | Início da vigência (SCD2) |
| `dt_fim` | DATE | Fim da vigência; `9999-12-31` se atual |
| `is_current` | BOOLEAN | `TRUE` para o registro vigente |
| `_batch_id` | VARCHAR(64) | Rastreabilidade |
| `_load_ts` | TIMESTAMPTZ | Quando foi carregado |

### `dim_estabelecimento` (Postgres `gold_dw` — SCD2)

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_estabelecimento` | SERIAL PK | Surrogate key |
| `co_cnes` | INT | Código CNES (7 dígitos) — chave natural |
| `nome_fantasia` | VARCHAR(120) | Nome fantasia |
| `razao_social` | VARCHAR(120) | Razão social |
| `tipo_gestao` | VARCHAR(10) | Tipo de gestão |
| `co_municipio` | INT | Município (IBGE) |
| `co_uf` | SMALLINT | Código UF |
| `snapshot_date` | DATE | Data do snapshot CNES |
| `dt_inicio / dt_fim / is_current` | DATE / BOOLEAN | SCD2 |

---

## 9. Padrões de Engenharia — Pre-commit Hooks

Configurados em `.pre-commit-config.yaml`. Três propriedades obrigatórias: **rápido** (< 5s), **determinístico**, **relevante**.

### Hooks configurados

| Hook | Ferramenta | O que detecta |
|---|---|---|
| `ruff` (linter) | `charliermarsh/ruff-pre-commit` | Erros de estilo, imports não usados, complexidade |
| `ruff-format` (formatter) | idem | Formatação Black-compatível |
| `yamllint` | `adrienverge/yamllint` | Indentação/sintaxe em YAML (Compose, CRDs, DAGs) |
| `detect-secrets` | `Yelp/detect-secrets` | Tokens AWS, chaves API, senhas hardcoded |
| `no-pii-in-code` | hook local pygrep | Referências a campos PII fora de arquivos autorizados |
| hooks padrão | `pre-commit-hooks` | trailing-whitespace, end-of-file, check-yaml, check-json, no-commit-to-branch |

### Hook anti-PII

Padrão monitorado:
```
(?i)(["'])(cpf|rg|nome_completo|data_nascimento|telefone|email_pessoal)(["'])
```

Exceções legítimas: `apps/oltp/jobs/extract_oltp_snapshot.py` e `apps/shared/masking.py` (adicionar `# noqa: PII` no topo).

### Configuração Ruff (`pyproject.toml`)

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP"]
ignore = ["E501"]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["N802"]
```

### Instalação

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files   # validar em todos os arquivos após clone
```

---

## 10. FAQ Técnico

**"Como você garante que o CPF não vaza para o Gold?"**
> "O mascaramento acontece no Silver, em `apps/shared/masking.py`. O job Silver chama `validate_no_pii()` obrigatoriamente antes de escrever — ele levanta `ValueError` se qualquer coluna PII sobreviver. Adicionalmente, o hook anti-PII no pre-commit bloqueia referências a campos PII em código fora dos dois arquivos autorizados. Temos também smoke tests que verificam que nenhuma coluna PII existe nas tabelas Bronze streaming e Gold."

**"Qual a diferença entre anonimização e pseudonimização?"**
> "Pseudonimização (GDPR/LGPD Art. 13) substitui identificadores por aliases — reversível com a chave. SHA-256 sem o salt é pseudonimização. Com o salt secreto armazenado em k8s Secret e sem acesso público, na prática é irreversível para atores externos, aproximando-se da anonimização. Tecnicamente, nosso esquema é pseudonimização forte."

**"O que acontece se o PII_SALT vazar?"**
> "Todos os hashes teriam de ser re-gerados com um novo salt. O processo é: rotacionar o secret k8s, reprocessar Bronze→Silver com `backfill` Airflow, e invalidar os hashes antigos. O impacto é operacional, não de privacidade imediata — o CPF real continua não exposto."
