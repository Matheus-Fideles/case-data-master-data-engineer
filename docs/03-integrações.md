# Guias de Integração — Fontes de Dados

> Visão consolidada das fontes. Guias detalhados (endpoints completos, paginação, armadilhas, schemas exatos) em [`integrations/`](./integrations/).

---

## Sumário

- [Princípios Comuns](#princípios-comuns)
- [1. Arboviroses (Dengue, Zika, Chikungunya) — SINAN](#1-arboviroses-dengue-zika-chikungunya--sinan)
- [2. SIM — Sistema de Informações sobre Mortalidade](#2-sim--sistema-de-informações-sobre-mortalidade)
- [3. CNES — Cadastro Nacional de Estabelecimentos de Saúde](#3-cnes--cadastro-nacional-de-estabelecimentos-de-saúde)
- [4. PNI — Vacinação (doses aplicadas 2024)](#4-pni--vacinação-doses-aplicadas-2024)
- [5. Municípios e Regiões de Saúde (IBGE/MS)](#5-municípios-e-regiões-de-saúde-ibgems)
- [6. OLTP Faker — Cadastro de Pacientes](#6-oltp-faker--cadastro-de-pacientes)
- [7. Stream Faker — Atendimentos em Tempo Real](#7-stream-faker--atendimentos-em-tempo-real)

---

## Princípios Comuns

1. **Modo offline por default** (`OFFLINE_MODE=1`). O projeto não pode depender de internet na execução local. Cada extrator respeita esse flag e usa o cache local em `data/raw/<fonte>/`.
2. **Determinismo:** fixtures e geradores Faker usam seed fixo declarado em `.env` para que reexecuções produzam o mesmo dataset.
3. **Idempotência:** repetir extração não gera duplicação; padrão `MERGE` por chave natural no Bronze.
4. **Metadados padrão em Bronze:** `_ingestion_ts`, `_batch_id`, `ano_mes` em todas as tabelas.
5. **Encoding UTF-8** garantido. APIs REST do MS retornam UTF-8; sem decodificação adicional.
6. **Separação extração ↔ ingestão** (ADR-008): Python extrai para `data/raw/` → Spark ingere para Bronze Delta.

---

## 1. Arboviroses (Dengue, Zika, Chikungunya) — SINAN

**Domínio:** `epidemiologico`  
**Tipo:** REST API JSON  
**Frequência:** mensal  

### Endpoints

```
GET https://apidadosabertos.saude.gov.br/arboviroses/dengue?nu_ano=2024
GET https://apidadosabertos.saude.gov.br/arboviroses/zikavirus?nu_ano=2024
GET https://apidadosabertos.saude.gov.br/arboviroses/chikungunya?nu_ano=2024
```

Sem autenticação. Resposta: `{ "parametros": [...registros...] }`.

### Paths Bronze

```
s3a://bronze/epidemiologico/dengue/
s3a://bronze/epidemiologico/zika/
s3a://bronze/epidemiologico/chikungunya/
```

Particionamento: `(nu_ano, sg_uf_not)`.

### Colunas principais

| Campo API | Coluna Bronze | Tipo | Notas |
|---|---|---|---|
| `id_agravo` | `id_agravo` | StringType | A90=dengue, A92=zika, A77=chikungunya |
| `dt_notific` | `dt_notific` | DateType | Data de notificação (YYYY-MM-DD) |
| `nu_ano` | `nu_ano` | IntegerType | Ano |
| `sg_uf_not` | `sg_uf_not` | StringType | UF de notificação |
| `id_municip` | `id_municipip` | StringType | Município notificação (6 dígitos) |
| `nu_idade_n` | `nu_idade_n` | StringType | Idade codificada — Silver decodifica |
| `cs_sexo` | `cs_sexo` | StringType | M/F/I |
| `classi_fin` | `classi_fin` | StringType | Classificação final |
| — | `_ingestion_ts` | TimestampType | Metadado padrão |
| — | `_batch_id` | StringType | `run_id` Airflow |

**Chave idempotência:** `(nu_ano, sg_uf_not, sem_not, nu_idade_n, cs_sexo, id_municip)` — notificações não têm ID único; usar hash composto.

### Armadilhas

- Paginação: API retorna até 1000 registros por request. Iterar com `offset` até resposta vazia.
- Campos de data com formato inconsistente nas bordas do ano (verificar no Bronze).
- API instável fora de horário comercial — usar cache offline.

---

## 2. SIM — Sistema de Informações sobre Mortalidade

**Domínio:** `hospitalar`  
**Tipo:** REST API JSON  
**Frequência:** mensal  

### Endpoint

```
GET https://apidadosabertos.saude.gov.br/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade
```

### Path Bronze

```
s3a://bronze/hospitalar/sim_obitos/
```

Particionamento: `(ano_obito, sg_uf_ocor)`.

### Colunas principais

| Campo API | Tipo | Notas |
|---|---|---|
| `causabas` | StringType | Causa básica do óbito (CID-10) |
| `idade` | StringType | Codificada (tipo+valor) |
| `sexo` | StringType | 1=Masculino, 2=Feminino, 9=Ignorado |
| `racacor` | StringType | Raça/cor |
| `codmunocor` | StringType | Município de ocorrência (6 dígitos) |
| `dtobito` | StringType | Data do óbito (DDMMAAAA) — Silver converte |
| `lococor` | StringType | Local de ocorrência (1=hospital, etc.) |

---

## 3. CNES — Cadastro Nacional de Estabelecimentos de Saúde

**Domínio:** `hospitalar`  
**Tipo:** REST API JSON  
**Frequência:** mensal  

### Endpoints

```
GET https://apidadosabertos.saude.gov.br/cnes/estabelecimentos
    ?limit=1000&offset=0&codigo_uf=35
GET https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/{cnes}
```

### Path Bronze

```
s3a://bronze/hospitalar/cnes_estabelecimentos/
```

Particionamento: `(co_uf, snapshot_date)`.

### Colunas principais

| Campo API | Tipo | Notas |
|---|---|---|
| `co_cnes` | IntegerType | Código CNES (7 dígitos) — chave natural |
| `no_fantasia` | StringType | Nome fantasia |
| `no_razao_social` | StringType | Razão social |
| `tp_gestao` | StringType | Tipo de gestão |
| `co_municipio_gestor` | IntegerType | Município (código IBGE 7 dígitos) |
| `co_uf` | SmallIntegerType | Código UF |
| `lat` / `lon` | DoubleType | Coordenadas geográficas |
| `tp_unidade` | StringType | Tipo de unidade (hospital=05, UPA=36, etc.) |
| `qt_leito_exist` | IntegerType | Leitos existentes |
| `qt_leito_sus` | IntegerType | Leitos SUS |

**Chave idempotência:** `co_cnes`.

### Armadilhas

- Paginação obrigatória: máx. 1000 por request, pode ter >10.000 registros nacionais.
- Coordenadas podem ser `null` para estabelecimentos sem geocodificação — tratar no Silver.
- Schema varia ligeiramente entre endpoints de lista e detalhe.

---

## 4. PNI — Vacinação (doses aplicadas 2024)

**Domínio:** `vacinal`  
**Tipo:** REST API JSON  
**Frequência:** mensal  

### Endpoint

```
GET https://apidadosabertos.saude.gov.br/vacinacao/doses-aplicadas-pni-2024
```

### Path Bronze

```
s3a://bronze/vacinal/vacinacao_pni/
```

Particionamento: `(ano_mes, sg_uf)`.

### Colunas principais

| Campo API | Tipo | Notas |
|---|---|---|
| `nk_codigo_documento` | StringType | Identificador do documento de vacinação |
| `codigo_vacina` | StringType | Código da vacina PNI |
| `sigla_vacina` | StringType | Ex.: PENTA, ROTA, HPV |
| `co_estabelecimento` | StringType | CNES do estabelecimento |
| `co_municipio_ibge` | StringType | Município (7 dígitos) |
| `dt_vacinacao` | DateType | Data de aplicação |
| `nu_idade_paciente` | IntegerType | Idade em anos |
| `raca_cor_paciente` | StringType | Raça/cor |
| `nk_paciente_hash` | StringType | Hash já fornecido pela API (sem PII) |

---

## 5. Municípios e Regiões de Saúde (IBGE/MS)

**Domínio:** `geografico`  
**Tipo:** REST API JSON  
**Frequência:** anual  

### Endpoint

```
GET https://apidadosabertos.saude.gov.br/macrorregiao-e-regiao-de-saude/municipio
```

### Path Bronze/Gold

```
s3a://bronze/geografico/municipios/
s3a://gold/geografico/dim_municipio/
```

Particionamento: nenhum (tabela pequena — ~5.570 municípios).

### Colunas principais

| Campo | Tipo | Notas |
|---|---|---|
| `co_municipio` | StringType | Código IBGE 7 dígitos |
| `no_municipio` | StringType | Nome do município |
| `sg_uf` | StringType | Sigla do estado |
| `no_uf` | StringType | Nome do estado |
| `no_regiao` | StringType | Região do país |
| `no_regiao_saude` | StringType | Região de saúde MS |
| `no_macrorregiao_saude` | StringType | Macrorregião de saúde MS |
| `qt_populacao` | IntegerType | Estimativa populacional IBGE 2022 |

---

## 6. OLTP Faker — Cadastro de Pacientes

**Domínio:** `oltp`  
**Tipo:** Banco Relacional (Postgres `oltp` schema)  
**Frequência:** diário (snapshot)  

### Geração dos dados

```bash
make seed   # executa apps/oltp/seed_faker.py — popula Postgres oltp.paciente
```

O seed usa `Faker('pt_BR')` com seed fixo para reprodutibilidade. **PII real nunca entra** — os dados são sintéticos desde a origem.

### Schema da tabela `oltp.paciente`

| Coluna | Tipo | PII? | Notas |
|---|---|---|---|
| `id_paciente` | UUID | não | Chave primária |
| `cpf` | VARCHAR(11) | **SIM** | Faker determinístico — mascarado no Silver |
| `nome_completo` | VARCHAR(120) | **SIM** | Faker — suprimido no Silver |
| `data_nascimento` | DATE | **SIM** | Faker — truncado para ano no Silver |
| `sexo` | CHAR(1) | não | M/F |
| `cep` | CHAR(8) | **SIM** | Faker — truncado para 3 primeiros dígitos no Silver |
| `municipio_codigo_ibge` | VARCHAR(7) | não | Vinculado à `dim_municipio` |
| `email` | VARCHAR(120) | **SIM** | Faker — suprimido no Silver |
| `telefone` | VARCHAR(20) | **SIM** | Faker — suprimido no Silver |
| `created_at` | TIMESTAMPTZ | não | |
| `updated_at` | TIMESTAMPTZ | não | Para CDC |

### Path Bronze

```
s3a://bronze/oltp/paciente/
```

Particionamento: `snapshot_date`.

**O mascaramento acontece no Silver**, em `apps/epidemiologico/jobs/silver_paciente.py`, usando `apps/shared/masking.py`:

```python
# CPF → SHA-256 + PII_SALT (k8s secret)
id_paciente_hash = SHA256(cpf + PII_SALT)
```

---

## 7. Stream Faker — Atendimentos em Tempo Real

**Domínio:** `streaming`  
**Tipo:** Kafka (eventos JSON)  
**Frequência:** contínua  

### Producer

```
apps/streaming/producer/main.py
```

Publica no tópico `notificacoes.raw` (configurável via `KAFKA_TOPIC`).

### Schema do evento Kafka

```json
{
  "id_atendimento": "<uuid>",
  "id_paciente_hash": "<sha256-64-chars>",
  "tipo_atendimento": "CONSULTA | URGENCIA | INTERNACAO",
  "cid10_principal": "J00",
  "ts_evento": "2024-01-15T14:30:00+00:00",
  "municipio_codigo_ibge": "3550308",
  "id_cnes": "<7-digit-cnes-code>"
}
```

**Nota:** `id_paciente_hash` já vem mascarado do producer — nunca há PII no Kafka.

### Consumer

```
apps/streaming/jobs/atendimento_consumer.py
```

- **Watermark:** 1 hora (eventos mais antigos são filtrados)
- **Bronze:** `s3a://bronze/streaming/atendimentos_stream/` (append + MERGE Delta)
- **Gold:** `s3a://gold/streaming/fatos_atendimento_stream/` (foreachBatch, particionado por `ano_mes`)
- **DLQ:** eventos com schema inválido vão para `KAFKA_DLQ_TOPIC`

### Tópicos Kafka

| Tópico | Partições | Retenção | Uso |
|---|---|---|---|
| `notificacoes.raw` | 3 | 7 dias | Eventos de atendimento |
| `notificacoes.dlq` | 1 | 30 dias | Dead Letter Queue — eventos inválidos |

### Rate do producer (ambiente local)

| Parâmetro | Valor padrão |
|---|---|
| `EVENTS_PER_SECOND` | 10 |
| `BATCH_SIZE` | 50 |
| `SLEEP_INTERVAL_MS` | 100 |

Ajustável via variáveis de ambiente no `docker-compose.yml` (perfil `streaming`).
