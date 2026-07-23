# Especificação de Schemas — Camada Bronze

> **Audiência:** quem vai codar `pipelines/common/schemas.py` e os jobs de ingestão Bronze.
> **Última revisão:** 2026-07-13 — fontes migradas de CSV/FTP DataSUS para REST API pública do Ministério da Saúde (`apidadosabertos.saude.gov.br`).

## Princípios da camada Bronze

1. **Bronze = raw + rastreabilidade.** Preserva o dado fonte com mínima transformação. Casts apenas onde inevitável.
2. **Schema explícito.** Cada tabela tem `StructType` declarado em `pipelines/common/schemas.py`; `mergeSchema=true` permite colunas novas.
3. **Particionamento previsível.** Cada tabela tem coluna(s) de partição estável.
4. **Idempotência por chave natural.** Cada tabela define `nk_*` para MERGE/dedupe.
5. **Metadados padrão em todas as tabelas Bronze:**
   - `_ingested_at` (TimestampType) — quando o job escreveu a linha
   - `_source_url` (StringType) — endpoint de origem (ex.: `/arboviroses/dengue`)
   - `_source_run_id` (StringType) — `run_id` do Airflow
6. **Encoding UTF-8 garantido.** API retorna UTF-8; sem decodificação adicional.
7. **Cache offline.** Extratores salvam JSON bruto em `data/raw/<fonte>/` antes de escrever Bronze. Demo funciona sem internet.

## Fluxo de extração → Bronze

```
REST API
  └─[PythonOperator]──→ data/raw/<fonte>/<ano_mes>.ndjson  (cache local)
       └─[DockerOperator · local[2]]──→ s3://bronze/<tabela>/<partição>/  (Delta)
```

A extração (Python) e a ingestão Bronze (Spark) são jobs separados conforme [ADR 0008](../architecture/decisions/0008-ingestion-layers.md).

---

## Tabelas Bronze

### 1. `bronze.dengue` ⭐ Fato principal

**Origem:** `GET /arboviroses/dengue?nu_ano=2024` (campo raiz: `parametros`)
**Particionamento:** `(nu_ano, sg_uf_not)`
**Chave idempotência:** `(nu_ano, sg_uf_not, sem_not, nu_idade_n, cs_sexo, id_municip)` — notificações não têm ID único; usar hash composto
**Path:** `s3a://bronze/dengue/nu_ano=2024/sg_uf_not=SP/`

| Coluna | Tipo Spark | Nullable | Campo API | Notas |
|---|---|---|---|---|
| `tp_not` | StringType | sim | `tp_not` | Tipo de notificação |
| `id_agravo` | StringType | não | `id_agravo` | CID-10 do agravo (A90=dengue) |
| `dt_notific` | DateType | não | `dt_notific` (`YYYY-MM-DD`) | Data de notificação |
| `sem_not` | StringType | sim | `sem_not` | Semana epidemiológica (AAAASS) |
| `nu_ano` | IntegerType | não | `nu_ano` | Ano da notificação |
| `sg_uf_not` | StringType | sim | `sg_uf_not` | UF de notificação |
| `id_municipip` | StringType | sim | `id_municipip` | Município de notificação (6 dígitos) |
| `id_regiona` | StringType | sim | `id_regiona` | Regional de saúde |
| `dt_sin_pri` | DateType | sim | `dt_sin_pri` | Data início sintomas |
| `sem_pri` | StringType | sim | `sem_pri` | Semana epidemiológica sintomas |
| `nu_idade_n` | StringType | sim | `nu_idade_n` | Idade codificada (Silver decodifica) |
| `cs_sexo` | StringType | sim | `cs_sexo` | M/F/I |
| `cs_gestant` | StringType | sim | `cs_gestant` | Gestante |
| `cs_raca` | StringType | sim | `cs_raca` | Raça/cor |
| `sg_uf` | StringType | sim | `sg_uf` | UF residência |
| `id_mn_resi` | StringType | sim | `id_mn_resi` | Município residência (6 dígitos) |
| `classi_fin` | StringType | sim | `classi_fin` | Classificação final |
| `dt_digita` | DateType | sim | `dt_digita` | Data digitação |
| `sg_uf_not` | StringType | não | partição | UF notificação |
| `nu_ano` | IntegerType | não | partição | Ano |
| `_ingested_at` | TimestampType | não | metadata | |
| `_source_url` | StringType | não | metadata | `/arboviroses/dengue` |
| `_source_run_id` | StringType | não | metadata | |

> Mesma estrutura para `bronze.zika` e `bronze.chikungunya` — apenas `id_agravo` difere (A92, A77).

---

### 2. `bronze.zika`

**Origem:** `GET /arboviroses/zikavirus?nu_ano=2024`
**Schema:** idêntico ao `bronze.dengue` (campo `id_agravo = 'A92'`)
**Path:** `s3a://bronze/zika/nu_ano=2024/sg_uf_not=SP/`

---

### 3. `bronze.chikungunya`

**Origem:** `GET /arboviroses/chikungunya?nu_ano=2024`
**Schema:** idêntico ao `bronze.dengue` (campo `id_agravo = 'A77'`)
**Path:** `s3a://bronze/chikungunya/nu_ano=2024/sg_uf_not=SP/`

---

### 4. `bronze.sim_obitos`

**Origem:** `GET /vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade` (campo raiz: `sim`)
**Particionamento:** `(ano_obito)`
**Chave idempotência:** `contador` (string — número sequencial do óbito)
**Path:** `s3a://bronze/sim_obitos/ano_obito=2024/`

| Coluna | Tipo Spark | Nullable | Campo API | Notas |
|---|---|---|---|---|
| `contador` | StringType | não | `contador` | Chave natural |
| `causabas` | StringType | sim | `causabas` | CID-10 causa básica |
| `horaobito` | StringType | sim | `horaobito` | HHMM |
| `idade` | StringType | sim | `idade` | Codificada (Silver decodifica) |
| `racacor` | StringType | sim | `racacor` | Raça/cor |
| `codmunres` | StringType | sim | `codmunres` | Município residência (6 dígitos) |
| `comunsvoim` | StringType | sim | `comunsvoim` | Município ocorrência (6 dígitos) |
| `lococor` | StringType | sim | `lococor` | Local ocorrência (1=hosp, 3=domicílio) |
| `assistmed` | StringType | sim | `assistmed` | Assistência médica (1=sim, 2=não) |
| `codestab` | StringType | sim | `codestab` | CNES do estabelecimento |
| `atestante` | StringType | sim | `atestante` | Tipo atestante |
| `dtrecebim` | StringType | sim | `dtrecebim` | Data recebimento (DDMMAAAA) |
| `dtcadastro` | DateType | sim | `dtcadastro` | Data cadastro (YYYY-MM-DD) |
| `linhab` | StringType | sim | `linhab` | Causa linha B |
| `linhaii` | StringType | sim | `linhaii` | Causa linha II |
| `ocup` | StringType | sim | `ocup` | Ocupação CBO |
| `ano_obito` | IntegerType | não | partição | Derivado de `dtcadastro` |
| `_ingested_at` | TimestampType | não | metadata | |
| `_source_url` | StringType | não | metadata | |
| `_source_run_id` | StringType | não | metadata | |

---

### 5. `bronze.vacinacao_pni`

**Origem:** `GET /vacinacao/doses-aplicadas-pni-2024` (campo raiz: `doses_aplicadas_pni`)
**Particionamento:** `(ano_mes_vacinacao, sigla_uf_estabelecimento)`
**Chave idempotência:** `codigo_documento`
**Path:** `s3a://bronze/vacinacao_pni/ano_mes=2024-01/uf=SP/`

| Coluna | Tipo Spark | Nullable | Campo API | Notas |
|---|---|---|---|---|
| `codigo_documento` | StringType | não | `codigo_documento` | UUID — chave natural |
| `codigo_paciente` | StringType | não | `codigo_paciente` | **Já hasheado pela fonte** (SHA-256) |
| `data_vacina` | DateType | não | `data_vacina` (`YYYY-MM-DD`) | Data de aplicação |
| `codigo_vacina` | StringType | sim | `codigo_vacina` | Código do imunobiológico |
| `sigla_vacina` | StringType | sim | `sigla_vacina` | Sigla (VPC10, COV, etc.) |
| `descricao_vacina` | StringType | sim | `descricao_vacina` | Nome completo |
| `codigo_vacina_fabricante` | StringType | sim | `codigo_vacina_fabricante` | |
| `descricao_vacina_fabricante` | StringType | sim | `descricao_vacina_fabricante` | |
| `codigo_cnes_estabelecimento` | StringType | sim | `codigo_cnes_estabelecimento` | CNES 7 dígitos |
| `nome_razao_social_estabelecimento` | StringType | sim | `nome_razao_social_estabelecimento` | |
| `codigo_municipio_estabelecimento` | StringType | sim | `codigo_municipio_estabelecimento` | 6 dígitos IBGE |
| `sigla_uf_estabelecimento` | StringType | sim | `sigla_uf_estabelecimento` | |
| `codigo_municipio_paciente` | StringType | sim | `codigo_municipio_paciente` | 6 dígitos IBGE |
| `sigla_uf_paciente` | StringType | sim | `sigla_uf_paciente` | |
| `numero_idade_paciente` | IntegerType | sim | `numero_idade_paciente` | Idade em anos |
| `codigo_raca_cor_paciente` | StringType | sim | `codigo_raca_cor_paciente` | |
| `nome_raca_cor_paciente` | StringType | sim | `nome_raca_cor_paciente` | |
| `descricao_estrategia_vacinacao` | StringType | sim | `descricao_estrategia_vacinacao` | Rotina, Campanha, etc. |
| `codigo_lote_vacina` | StringType | sim | `codigo_lote_vacina` | |
| `ano_mes_vacinacao` | StringType | não | partição | Derivado de `data_vacina` |
| `sigla_uf_estabelecimento` | StringType | não | partição | |
| `_ingested_at` | TimestampType | não | metadata | |
| `_source_url` | StringType | não | metadata | |
| `_source_run_id` | StringType | não | metadata | |

---

### 6. `bronze.cnes_estabelecimentos`

**Origem:** `GET /cnes/estabelecimentos` (campo raiz: `estabelecimentos`, paginado com `limit`/`offset`)
**Particionamento:** `(snapshot_date)`
**Chave idempotência:** `codigo_cnes`
**Path:** `s3a://bronze/cnes_estabelecimentos/snapshot_date=YYYY-MM-DD/`

| Coluna | Tipo Spark | Nullable | Campo API |
|---|---|---|---|
| `codigo_cnes` | LongType | não | `codigo_cnes` |
| `nome_razao_social` | StringType | sim | `nome_razao_social` |
| `nome_fantasia` | StringType | sim | `nome_fantasia` |
| `codigo_tipo_unidade` | IntegerType | sim | `codigo_tipo_unidade` |
| `codigo_uf` | IntegerType | sim | `codigo_uf` |
| `codigo_municipio` | LongType | sim | `codigo_municipio` |
| `descricao_esfera_administrativa` | StringType | sim | `descricao_esfera_administrativa` |
| `latitude_estabelecimento_decimo_grau` | DoubleType | sim | `latitude_estabelecimento_decimo_grau` |
| `longitude_estabelecimento_decimo_grau` | DoubleType | sim | `longitude_estabelecimento_decimo_grau` |
| `estabelecimento_possui_atendimento_hospitalar` | IntegerType | sim | `estabelecimento_possui_atendimento_hospitalar` |
| `codigo_identificador_turno_atendimento` | StringType | sim | `codigo_identificador_turno_atendimento` |
| `data_atualizacao` | DateType | sim | `data_atualizacao` |
| `snapshot_date` | DateType | não | partição |
| `_ingested_at` | TimestampType | não | metadata |
| `_source_url` | StringType | não | metadata |
| `_source_run_id` | StringType | não | metadata |

---

### 7. `bronze.municipios`

**Origem:** `GET /macrorregiao-e-regiao-de-saude/municipio` (campo raiz: `macrorregiao_regiao_saude_municipios`)
**Particionamento:** `(snapshot_date)`
**Chave idempotência:** `codigo_municipio`
**Path:** `s3a://bronze/municipios/snapshot_date=YYYY-MM-DD/`

| Coluna | Tipo Spark | Nullable | Campo API |
|---|---|---|---|
| `codigo_municipio` | StringType | não | `codigo_municipio` (6 dígitos) |
| `municipio` | StringType | não | `municipio` |
| `codigo_uf` | StringType | sim | `codigo_uf` |
| `uf` | StringType | sim | `uf` |
| `regiao_pais` | StringType | sim | `regiao_pais` |
| `codigo_regiao_pais` | StringType | sim | `codigo_regiao_pais` |
| `codigo_regiao_saude` | StringType | sim | `codigo_regiao_saude` |
| `regiao_saude` | StringType | sim | `regiao_saude` |
| `codigo_macrorregiao_saude` | StringType | sim | `codigo_macrorregiao_saude` |
| `macrorregiao_saude` | StringType | sim | `macrorregiao_saude` |
| `populacao_estimada_ibge_2022` | LongType | sim | `populacao_estimada_ibge_2022` |
| `snapshot_date` | DateType | não | partição |
| `_ingested_at` | TimestampType | não | metadata |
| `_source_url` | StringType | não | metadata |
| `_source_run_id` | StringType | não | metadata |

---

### 8. `bronze.oltp_paciente` ⚠️ CONTÉM PII

**Origem:** `SELECT * FROM oltp.paciente` no Postgres (schema `oltp`) — dados sintéticos gerados por Faker
**Particionamento:** `(snapshot_date)`
**Chave idempotência:** `id_paciente`
**Path:** `s3a://bronze/oltp_paciente/snapshot_date=YYYY-MM-DD/`

> 🔒 **CRITICAL:** única tabela Bronze com PII em claro. RBAC Bronze restringe acesso. Silver mascara antes de propagar. Ver [ADR 0004](../architecture/decisions/0004-pii-masking.md).

| Coluna | Tipo Spark | Nullable | Notas |
|---|---|---|---|
| `id_paciente` | LongType | não | bigserial |
| `cpf` | StringType | não | **PII** — 11 dígitos |
| `nome` | StringType | não | **PII** |
| `data_nascimento` | DateType | não | **PII (generalizado no Silver)** |
| `sexo` | StringType | não | M / F / O |
| `cep` | StringType | não | **PII (truncado no Silver)** — 8 dígitos |
| `municipio_codigo_ibge` | StringType | não | 6 dígitos |
| `email` | StringType | sim | **PII (suprimido no Silver)** |
| `telefone` | StringType | sim | **PII (suprimido no Silver)** |
| `created_at` | TimestampType | não | |
| `updated_at` | TimestampType | não | Detecção de mudança SCD2 |
| `snapshot_date` | DateType | não | partição |
| `_ingested_at` | TimestampType | não | metadata |
| `_source_url` | StringType | não | `postgres://oltp/paciente` |
| `_source_run_id` | StringType | não | metadata |

---

### 9. `bronze.atendimentos_stream`

**Origem:** Kafka topic `notificacoes.raw` — eventos gerados pelo container `stream-producer` (Faker)
**Particionamento:** `(ano_mes_dia)` derivado de `ts_evento`
**Chave idempotência:** `(id_atendimento, ts_evento)`
**Path:** `s3a://bronze/atendimentos_stream/ano_mes_dia=YYYY-MM-DD/`
**Engine:** Spark Structured Streaming (pod k8s persistente)

| Coluna | Tipo Spark | Nullable | Notas |
|---|---|---|---|
| `id_atendimento` | StringType | não | UUID v4 |
| `id_paciente` | LongType | não | FK → `oltp.paciente` |
| `cnes_estabelecimento` | StringType | não | 7 dígitos |
| `ts_evento` | TimestampType | não | Event time (UTC) |
| `tipo_atendimento` | StringType | não | PS / consulta / urgência |
| `sintomas_cid` | ArrayType(StringType) | sim | Lista de CIDs |
| `triagem` | StringType | sim | Verde / Amarela / Vermelha |
| `kafka_partition` | IntegerType | não | Spark Kafka source |
| `kafka_offset` | LongType | não | Spark Kafka source |
| `kafka_timestamp` | TimestampType | não | Processing time |
| `ano_mes_dia` | DateType | não | partição |
| `_ingested_at` | TimestampType | não | metadata |
| `_source_run_id` | StringType | não | query_id do Structured Streaming |

---

## Consolidação — visão executiva

| Tabela Bronze | Fonte | PII? | Particionamento | Frequência |
|---|---:|---|---|---|
| `dengue` | API REST | não | (nu_ano, sg_uf_not) | anual |
| `zika` | API REST | não | (nu_ano, sg_uf_not) | anual |
| `chikungunya` | API REST | não | (nu_ano, sg_uf_not) | anual |
| `sim_obitos` | API REST | não direto | (ano_obito) | anual |
| `vacinacao_pni` | API REST | `codigo_paciente` já hash | (ano_mes, uf) | mensal |
| `cnes_estabelecimentos` | API REST | não | (snapshot_date) | mensal |
| `municipios` | API REST | não | (snapshot_date) | trimestral |
| `oltp_paciente` | Postgres Faker | **SIM** | (snapshot_date) | diário |
| `atendimentos_stream` | Kafka/Faker | parcial | (ano_mes_dia) | contínuo |
