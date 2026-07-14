# Integração: Postgres OLTP simulado — Seed com Faker

> **Sprint:** S1 (estrutura) + S2 (volume) + S3 (CDC simulado)
> **Requisitos cobertos:** Extração via banco de dados · Segurança · Mascaramento (PII real)

## 1. Visão geral

Esta "fonte" é interna: um **Postgres simulando um sistema OLTP de cadastro de pacientes e estabelecimentos**, populado por seeds com a biblioteca **Faker**. A razão de existir é **fornecer PII real (CPF, nome, endereço, telefone)** para demonstrar a camada de **segurança e mascaramento** que o enunciado exige.

Diferente das outras fontes (DataSUS, CNES, IBGE), aqui **o candidato controla 100% dos dados**: pode definir volume, distribuição e até inserir **mudanças temporais** que validam o **SCD Tipo 2** das dimensões `dim_paciente` e `dim_estabelecimento`.

**Por que está no case:**
- Cobre o requisito de extração via **banco relacional** (variedade)
- Provê **PII verdadeiro** (gerado, mas com formato real e checksums válidos) — defende mascaramento real, não cosmético
- Permite simular **CDC** (mudança de CEP do paciente, mudança de leitos do hospital) para testar SCD2
- Reprodutível — `make seed` em qualquer máquina gera o mesmo dataset

## 2. Schema do Postgres OLTP

> Já implementado em `infra/postgres/init/01_schema_oltp.sql`. Carrega no boot do `postgres-source`.

### `oltp.paciente`

| Coluna | Tipo | Descrição | Sensível? |
|---|---|---|---|
| `id_paciente` | bigserial PK | Surrogate sequencial | Não |
| `cpf` | char(11) UNIQUE | CPF com checksum válido (sem pontuação) | **PII** |
| `nome` | varchar(120) | Nome completo | **PII** |
| `data_nascimento` | date | — | **PII (geral.)** |
| `sexo` | char(1) | M, F, O | Não |
| `cep` | char(8) | Sem hífen | **PII (geral.)** |
| `municipio_codigo_ibge` | char(7) | 7 dígitos IBGE | Não |
| `email` | varchar(120) NULL | — | **PII** |
| `telefone` | varchar(20) NULL | — | **PII** |
| `created_at` | timestamptz default now() | — | Não |
| `updated_at` | timestamptz default now() | Atualizado em cada UPDATE | Não |

### `oltp.estabelecimento`

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_estabelecimento` | bigserial PK | Surrogate |
| `cnes` | char(7) UNIQUE | Código CNES |
| `nome` | varchar(200) | Nome fantasia |
| `municipio_codigo_ibge` | char(7) | Localização |
| `tipo_unidade` | varchar(50) | Hospital, UPA, etc. |
| `tem_uti` | boolean | Capacidade UTI |

## 3. Estratégia de seed

### Biblioteca: **Faker** com locale `pt_BR`

```bash
pip install Faker faker-commerce
```

Recomendações:
- **Sempre setar seed** (`Faker.seed(42)` e `random.seed(42)`) — reproducibilidade é critério da banca
- **Usar locale `pt_BR`** — gera nomes brasileiros, CEPs reais, CPFs com checksum válido
- **Bulk insert via COPY** ou `psycopg.executemany` para performance — 1000 INSERTs simples levam minutos

### Volumes-alvo

| Sprint | Pacientes | Estabelecimentos | Updates SCD2 |
|---|---|---|---|
| S1 | 1.000 | 50 | 0 |
| S2 | 50.000 | 500 | 0 |
| S3 | 50.000 | 500 | 5–10% (UPDATE de cep, leitos) |

S2 é o volume "demo realista" — cabe em laptop, demonstra processamento distribuído sem virar piada (1k linhas é trivial demais).

### Determinismo de CPF

Faker `pt_BR` já gera CPF com **checksum válido** (não é só 11 dígitos aleatórios). Verifique no método `.cpf()`. Se quiser remover pontuação:

```python
fake.cpf().replace(".", "").replace("-", "")  # → "12345678909"
```

> **Importante:** o CPF gerado é **sintético**. Não corresponde a pessoa real. Mas **tem o formato e checksum corretos**, o que valida o pipeline de hash/mascaramento como se fosse real.

### Distribuição realista

Para o dashboard ficar interessante:
- **Distribuir municípios** com peso proporcional à população (use `dim_municipio.populacao` do IBGE depois de carregada como tabela auxiliar — ou hardcode top 100 municípios na primeira rodada)
- **Distribuir idade** com curva semelhante à pirâmide etária BR (20% 0–14, 65% 15–64, 15% 65+)
- **Distribuir sexo** ~50/50 (M/F) com pequena fração 'O'
- **Estabelecimentos** concentrados em capitais (~30% em SP, RJ, BH, etc.)

### Pitfalls do Faker

| Pitfall | Mitigação |
|---|---|
| `fake.email()` gera duplicatas em volumes grandes | Adicionar sufixo numérico ou usar `unique` |
| `fake.cpf()` raramente colide, mas pode | Wrapper que checa unicidade antes de inserir |
| `fake.cep()` retorna formato `00000-000` (com hífen) | Normalizar com `.replace("-", "")` para char(8) |
| Locale `pt_BR` mais lento que `en_US` | Aceitável; gerar em paralelo se for problema |

## 4. CDC simulado para SCD2

Na **S3**, depois do seed inicial, rodar um script de **mudanças graduais** que:

| Operação | % do volume | Exemplo |
|---|---|---|
| UPDATE paciente.cep | 5% | Mudança de endereço |
| UPDATE paciente.email | 2% | Mudança de e-mail |
| UPDATE estabelecimento.leitos | 10% | Reforma hospitalar |
| UPDATE estabelecimento.tem_uti | 1% | Hospital ganha UTI |
| INSERT novo paciente | +500 | Crescimento gradual |
| (não fazer DELETE) | 0% | Soft-delete só, mas não usamos no case |

Cada UPDATE deve atualizar `updated_at = now()` (já feito por TRIGGER no init.sql ou manualmente).

A extração para Bronze deve **detectar mudanças via `updated_at > last_extraction_ts`** (CDC pobre por timestamp; Debezium é evolução futura). O Silver compara hash de atributos para decidir se vira nova versão SCD2.

## 5. Estratégia de extração para Bronze

### Caminho

```
Postgres OLTP  ──(extrator)──▶  data/raw/oltp/snapshot_<ts>/{paciente,estabelecimento}.parquet
                                              │
                                              ▼
                                  Bronze (s3a://bronze/oltp/...)
```

### Biblioteca: **psycopg** (3.x) ou **SQLAlchemy** + **pyarrow**

```bash
pip install "psycopg[binary]" pyarrow
```

`psycopg.copy_to` é a forma mais rápida; alternativa é `pandas.read_sql` para volumes pequenos.

### Padrão (semântica)

A função deve aceitar:
- `since_ts` (timestamptz, default = início dos tempos para full snapshot, ou último checkpoint para incremental)
- `output_path` (s3a://bronze/oltp/snapshot_date=YYYY-MM-DD/)

Retornar:
- Contagem de linhas extraídas
- Timestamp do snapshot

## 6. Particionamento e idempotência

### Path no MinIO

```
s3a://bronze/oltp/paciente/snapshot_date=YYYY-MM-DD/<arquivo>.parquet
s3a://bronze/oltp/estabelecimento/snapshot_date=YYYY-MM-DD/<arquivo>.parquet
```

### Modo de escrita

- **`INSERT OVERWRITE PARTITION (snapshot_date)`** — uma partição por extração
- Silver compara snapshot atual com anterior para alimentar SCD2

### Chave de idempotência

- `id_paciente` para pacientes
- `cnes` para estabelecimentos

## 7. Cache offline

Não aplicável — o Postgres está local no Compose. O "cache" é o próprio volume `pg_source_data`. Para CI:
- Rodar `make seed` no setup do job
- Container Postgres é determinístico graças ao seed Faker

Em situações onde Postgres não esteja disponível (improvável no nosso fluxo), cachear snapshots em `data/raw/oltp/snapshot_<seed>/`.

## 8. Sugestão de fluxo de seed

```
make up-s1                     # sobe postgres-source vazio
↓
scripts/seed_data.sh           # candidato implementa: roda script Python que insere via Faker
                               #   - 1k pacientes (S1) ou 50k (S2)
                               #   - 50 estabelecimentos
                               #   - opcional: simulate_changes() para SCD2
↓
psql ... -c "SELECT count(*) FROM oltp.paciente"   # validação manual
```

Critérios da função `seed`:
- Idempotente: rodar duas vezes deve dar **resultado idêntico** (TRUNCATE + INSERT, não append)
- Determinística: seed fixo
- Rápida: <30s para 1k linhas, <2min para 50k linhas
- Configurável: aceita `n_pacientes` por env var

## 9. Considerações de segurança e LGPD

> A política completa fica em [`docs/security.md`](../security.md). Aqui só o essencial.

- **Os dados são sintéticos**, mas **tratamos como se fossem reais** durante toda a pipeline. Isso valida o framework LGPD-aderente.
- Salt do hash de CPF deve vir de **variável de ambiente** (`PII_HASH_SALT`), nunca hardcoded.
- Em produção real, esse Postgres seria atrás de **firewall + bastion + RBAC + auditoria**. No case, `app_reader` (somente leitura) é o role que o extractor usa — `app_writer` é só para o seed.
- O Bronze contém PII em claro. **Acesso ao bucket Bronze é restrito** (no case, MinIO sem público; Trino RBAC só permite Silver e Gold para `analyst`).

## 10. Pitfalls comuns

| Pitfall | Sintoma | Mitigação |
|---|---|---|
| Esquecer seed determinístico | Cada `make seed` gera CPFs/nomes diferentes | Sempre `Faker.seed(42)` e `random.seed(42)` |
| Volume grande quebra `psycopg.executemany` | Lento ou OOM | Usar `psycopg.copy_to` (10x mais rápido) |
| `created_at` e `updated_at` iguais no seed inicial | Não distingue insert de update | Usar `now() - random.timedelta(days=...)` para `created_at` |
| Falta TRIGGER de `updated_at` | UPDATE não atualiza timestamp | Implementar TRIGGER ou setar manualmente |
| CPF colidindo entre execuções de seeds em paralelo | UNIQUE constraint viola | Distinguir por `random_state` diferente, ou `TRUNCATE` antes |
| Locale `pt_BR` em ambiente sem ICU | `LookupError` | Container Alpine: instalar `tzdata` e `icu-libs` |
| Passar volume grande para Faker e travar memória | OOM no seeder | Insert em chunks de 5k linhas |

## 11. Referências

- **Faker docs:** <https://faker.readthedocs.io/>
- **Faker pt_BR providers:** <https://faker.readthedocs.io/en/master/locales/pt_BR.html>
- **psycopg 3:** <https://www.psycopg.org/psycopg3/docs/>
- **Postgres COPY (perf):** <https://www.postgresql.org/docs/current/sql-copy.html>
- **CDC com Debezium (evolução futura):** <https://debezium.io/documentation/reference/stable/connectors/postgresql.html>
