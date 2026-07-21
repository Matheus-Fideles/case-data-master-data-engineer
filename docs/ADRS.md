# ADRs — Architecture Decision Records

> Registro de todas as decisões arquiteturais do case Academia Santander Data Engineering.
> Cada ADR documenta **contexto**, **opções avaliadas**, **decisão** e **consequências**.

---

## Índice

### Arquitetura de Processamento
- [ADR-001 — Arquitetura Lambda vs Kappa](#adr-001--arquitetura-lambda-vs-kappa)
- [ADR-007 — Spark on Kubernetes (Rancher Desktop)](#adr-007--spark-on-kubernetes-rancher-desktop)
- [ADR-008 — Separação de Camadas de Ingestão](#adr-008--separação-de-camadas-de-ingestão)

### Qualidade e Confiabilidade de Dados
- [ADR-002 — Estratégia de Idempotência e MERGE](#adr-002--estratégia-de-idempotência-e-merge)
- [ADR-003 — Política de Schema Evolution](#adr-003--política-de-schema-evolution)
- [ADR-005 — Watermark e Tratamento de Eventos Atrasados](#adr-005--watermark-e-tratamento-de-eventos-atrasados)
- [ADR-006 — Implementação de SCD Tipo 2](#adr-006--implementação-de-scd-tipo-2)

### Segurança e Privacidade
- [ADR-004 — Estratégia de Mascaramento de PII](#adr-004--estratégia-de-mascaramento-de-pii)

### Padrões Arquiteturais
- [ADR-009 — Data Mesh: Organização por Domínios](#adr-009--data-mesh-organização-por-domínios)
- [ADR-010 — Arquitetura Hexagonal (Ports & Adapters)](#adr-010--arquitetura-hexagonal-ports--adapters)
- [ADR-011 — Serving Layer: Trino + HMS + Delta Lake](#adr-011--serving-layer-trino--hms--delta-lake)

---

# ADR 0001 — Arquitetura Lambda vs Kappa

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza
- **Contexto do case:** Academia Santander — Engenharia de Dados

## Contexto

O case exige que a solução seja capaz de ingerir dados de múltiplas fontes (CSV, APIs públicas, banco de dados relacional) com **diferentes características de chegada**:

| Fonte | Cadência natural | Volume típico |
|---|---|---|
| SIH-SUS (internações DataSUS) | Lote mensal (arquivos consolidados) | Centenas de MB por UF/mês |
| SIM (mortalidade DataSUS) | Lote anual/mensal | Centenas de MB |
| CNES (estabelecimentos) | API REST, atualização mensal | KB-MB |
| IBGE (população municipal) | API REST, atualização anual | KB |
| Postgres OLTP (cadastro de pacientes simulado) | Mudanças contínuas (CDC) | Linhas por minuto |
| Stream simulado de atendimentos (Faker) | Eventos contínuos (tempo real) | Eventos por segundo |

A o case menciona explicitamente "arquiteturas Kappa, Lambda" no requisito 2 (Ingestão), portanto a decisão precisa ser **declarada e justificada**.

## Opções avaliadas

### Opção A — Arquitetura Lambda (escolhida)

Dois caminhos paralelos com convergência no Gold:

- **Camada batch:** Airflow orquestra extração mensal/diária de CSV DataSUS, APIs CNES/IBGE e snapshots do Postgres OLTP → MinIO Bronze → PySpark Silver → modelagem dimensional Gold.
- **Camada speed (streaming):** Producer simulado publica atendimentos em Kafka → Spark Structured Streaming consome → grava direto na Bronze (Delta Lake append) → janela de agregação alimenta tabela Gold de "atendimentos em tempo real".
- **Convergência:** Trino consulta Gold (batch) + Gold (speed) na mesma camada lógica; Metabase exibe ambos em dashboards.

**Vantagens neste case:**
- Honra a natureza real das fontes: DataSUS publica em lote consolidado mensal; forçar streaming nesse caminho é artificial e tecnicamente inconsistente.
- Permite mostrar **dois pipelines distintos** funcionando ao vivo na demo de 1h30, evidenciando domínio dos dois paradigmas.
- Falha de um caminho não derruba o outro (resiliência).

**Desvantagens:**
- Duplicação de lógica de negócio entre batch e speed (atenuada com biblioteca compartilhada `pipelines/common/`).
- Mais código para manter (mitigado pelo escopo limitado do case).

### Opção B — Arquitetura Kappa (rejeitada)

Único caminho via streaming. CSVs do DataSUS seriam "rebobinados" (replay) através de Kafka como se fossem eventos.

**Por que foi rejeitada:**
- Modelo conceitualmente forçado: arquivos do DataSUS são publicações batch consolidadas mensais, não eventos contínuos. Ingerir 30 dias num único Kafka topic e fingir que são eventos compromete corretude técnica e é difícil de justificar tecnicamente.
- Infraestrutura mais cara: backfill histórico via stream exige Kafka com retenção longa (TB) — inviável em laptop.
- Esconde a competência batch (orquestração, dependências entre tarefas, idempotência) que a é esperado ver.

### Opção C — Apenas batch (rejeitada)

Ignorar streaming completamente.

**Por que foi rejeitada:**
- Requisito 2 cita "tempo real" explicitamente.
- Requisito 8 cita "demanda crescente por análises em tempo real".
- A Pergunta esperada: como a solução lida com streaming.

## Decisão

**Adotar Arquitetura Lambda** com:

1. **Batch layer:** Airflow + PySpark + MinIO/Delta + Postgres DW.
2. **Speed layer:** Kafka + Spark Structured Streaming + Delta append + agregação por janela.
3. **Serving layer:** Trino federando Gold batch e Gold speed; Metabase como ponto único de visualização.

## Consequências

### Positivas
- Resposta direta e correta às perguntas técnicas sobre Kappa vs Lambda.
- Demonstração visual de dois pipelines distintos vivos na apresentação.
- Resiliência: falha em um caminho não interrompe o outro.
- Fontes batch e stream tratadas conforme sua natureza real.

### Negativas (e mitigações)
- **Duplicação de lógica:** mitigada por `pipelines/common/` (mascaramento, validações, lineage compartilhados entre batch e stream).
- **Custo cognitivo de duas pipelines:** mitigado pelo escopo enxuto (1 fato batch principal + 1 fato streaming).
- **Reconciliação batch×stream:** as duas camadas escrevem em tabelas Gold separadas (`gold.fato_internacao` e `gold.fato_atendimento_stream`) — não há merge complexo neste case.

## Evolução futura (slide cloud)

Em produção AWS:
- Batch layer: EMR + S3 + Glue Catalog + Redshift.
- Speed layer: MSK + Kinesis Data Analytics ou EMR Streaming.
- Serving: Athena (federado) + QuickSight.

## Referências
- Marz, N. & Warren, J. *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*.
- Kreps, J. *"Questioning the Lambda Architecture"* (argumentação em favor de Kappa) — referenciada para mostrar conhecimento dos dois lados.

---

# ADR 0002 — Estratégia de Idempotência e MERGE

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza

## Contexto

Pipelines de dados falham — rede cai, cluster reinicia, container morre. **Reexecutar a mesma carga sem corromper o resultado** é requisito mínimo de qualquer plataforma séria. A Pergunta relevante::

> *"Se o seu DAG do SIH-SUS rodar duas vezes para o mesmo mês, o que acontece com os dados? E se o consumer Kafka receber a mesma mensagem duplicada?"*

Sem resposta clara, perdemos pontos de **corretude técnica**. Esta decisão registra a estratégia por camada, por tipo de fonte e por modo de carga.

## Princípios

1. **Idempotência é função da chave + modo de escrita.** Sem chave natural ou surrogate determinístico, idempotência não existe.
2. **Cada camada tem seu padrão.** Bronze prefere `INSERT OVERWRITE PARTITION`; Silver/Gold preferem `MERGE`.
3. **Streaming exige checkpoint + transação.** Sem `foreachBatch` + `MERGE` Delta, exactly-once é ilusão.
4. **Idempotência ≠ resiliência.** Idempotência protege contra **reexecução**; resiliência (retry, DLQ) protege contra **falha externa**. As duas convivem.

## Opções avaliadas — modo de escrita por camada

### Bronze — `INSERT OVERWRITE PARTITION` ✅

**Padrão:** ao reprocessar, **sobrescreve a partição inteira** correspondente ao período (ex.: `(uf, ano_mes) = ('SP', '2024-01')`).

**Por que:**
- DataSUS publica arquivos consolidados — não há "evento parcial" a se preservar
- Reprocessar SP/2024-01 deve dar o mesmo resultado que a primeira execução
- Operação atômica em Delta (transação na partição); leitores em curso veem snapshot anterior até commit
- Performance: melhor que MERGE quando 100% da partição é reescrita
- Simples de raciocinar

**Por que não MERGE no Bronze:**
- MERGE precisa de chave natural; arquivos DataSUS têm `N_AIH`, mas o overhead de MERGE para reprocessamento de 250 mil linhas não compensa
- MERGE é caro quando o objetivo é **substituir tudo**, não reconciliar deltas

**Por que não APPEND no Bronze:**
- Reexecução duplicaria linhas — quebra a invariante
- Filtros por `_ingested_at` para deduplicar funcionam mas inflam o storage e complicam queries

### Silver e Gold — `MERGE INTO ... USING ... ON ...` ✅

**Padrão:** match por chave natural; INSERT se não existe, UPDATE se mudou, NO-OP se igual.

**Por que:**
- Silver/Gold são **modelos canônicos** — preservar histórico de carga não tem valor analítico, mas atualizar registros existentes (ex.: nova versão SCD2 de paciente) é parte da modelagem
- `MERGE` Delta é atômico, idempotente por construção (mesma chave, mesma decisão)
- Permite SCD Tipo 2 (com cláusulas `WHEN MATCHED AND <hash mudou>`) sem código complicado

**Padrão de MERGE Delta (semântica para implementação):**

```sql
MERGE INTO silver.fato_internacao tgt
USING staging_sih_sus_silver src
   ON tgt.nk_aih = src.nk_aih
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...)
```

A coluna `_row_hash` (MD5 ou SHA-256 das colunas relevantes) detecta mudança real e evita rewrites quando nada mudou — ganho de I/O.

### Streaming Bronze — append + dedupe via `foreachBatch` ✅

**Padrão:** consumer Spark Structured Streaming escreve **append** com dedupe explícita por chave de evento dentro do `foreachBatch`.

**Por que:**
- Append é o modo nativo de Structured Streaming
- Kafka pode reentregar (at-least-once por default no producer; idempotent producer ainda pode duplicar entre brokers)
- `foreachBatch` recebe DataFrame do micro-batch → faz `MERGE` Delta com `(id_paciente, ts_evento)` como chave
- Garante **exactly-once efetivo**, mesmo com reentrega do Kafka

**Configurações críticas Spark Streaming:**

| Configuração | Valor | Razão |
|---|---|---|
| `enable.idempotence` (no producer Kafka) | `true` | Evita duplicação no nível do broker |
| `processingTime` trigger | `30 seconds` | Responsivo na demo, não martela CPU |
| `checkpointLocation` | `s3a://landing/_checkpoints/atendimentos_stream/` | Sem isso, restart relê tudo |
| Watermark | `1 hour` | Vide [`ADR 0005`](./0005-streaming-watermark.md) |

## Decisão por fonte/tabela

| Fonte | Camada | Modo | Chave de idempotência |
|---|---|---|---|
| DataSUS SIH-SUS | Bronze | `INSERT OVERWRITE PARTITION (uf, ano_mes)` | `nk_aih` (validação) |
| DataSUS SIM | Bronze | `INSERT OVERWRITE PARTITION (uf, ano)` | `nk_dec_obito` |
| API CNES | Bronze | `INSERT OVERWRITE PARTITION (snapshot_date)` | `codigo_cnes` |
| API IBGE população | Bronze | `INSERT OVERWRITE PARTITION (ano)` | `(codigo_municipio_7, ano)` |
| API IBGE localidades | Bronze | `INSERT OVERWRITE PARTITION (snapshot_date)` | `codigo_municipio_7` |
| OLTP Postgres pacientes | Bronze | `INSERT OVERWRITE PARTITION (snapshot_date)` | `id_paciente` |
| OLTP Postgres estabelecimento | Bronze | `INSERT OVERWRITE PARTITION (snapshot_date)` | `cnes` |
| Stream atendimentos (Kafka) | Bronze | append + `foreachBatch` MERGE | `(id_paciente, ts_evento)` |
| Silver fatos | Silver | `MERGE` por nk + `_row_hash` | `nk_*` |
| Silver dimensões | Silver | `MERGE` SCD2 (vide [ADR 0006](./0006-scd2.md)) | `nk_*` + janela temporal |
| Gold fatos | Gold | `MERGE` por nk | `nk_*` |
| Gold dimensões | Gold | `MERGE` SCD2 | `nk_*` + janela |

## Reprocessamento — três cenários

### Cenário 1: Reexecução simples (mesma execução, mesma janela)

`make airflow-trigger DAG=batch_datasus_sih CONF='{"uf":"SP","ano_mes":"2024-01"}'`

Resultado esperado: idêntico ao da primeira execução. Storage não cresce. Linhas em downstream (Silver, Gold) detectam que nada mudou (`_row_hash` igual) e MERGE vira NO-OP — ganho de I/O.

### Cenário 2: Backfill histórico (reprocessar mês passado)

Mesma execução com `ano_mes` diferente. A partição `(uf=SP, ano_mes=2023-12)` é sobrescrita; partições anteriores ficam intactas.

**Cuidado:** se uma fonte mudou entre a primeira carga e o backfill (ex.: DataSUS publicou correção do arquivo), o backfill propaga a versão atual. Documentar é a única proteção: log do `_source_file` checksum em metadata permite identificar.

### Cenário 3: Reprocessamento ampliado por mudança de schema/lógica

Caso: descobrimos bug no extrator que perdia o campo `etnia`. Após correção:

1. Reprocessar Bronze de TODOS os meses afetados (`for ano_mes in range(2018-01, 2024-01): trigger`)
2. Reprocessar Silver consequente (cascata via Airflow)
3. Reprocessar Gold (idem)
4. **Métrica de drift Silver↔Gold** vai sinalizar diferença até a propagação completar

## Padrões anti-corretos (evitados explicitamente)

| Anti-padrão | Por que é ruim |
|---|---|
| `INSERT INTO bronze.sih_sus` (append puro, sem dedupe) | Reexecução duplica linhas |
| `DELETE FROM bronze.sih_sus WHERE ano_mes = ? ; INSERT INTO bronze.sih_sus VALUES (...)` | Janela onde a partição não existe; leitores concorrentes leem zero |
| `MERGE` no Bronze por linha individual | Caro vs OVERWRITE quando partição inteira muda |
| Confiar só em `enable.idempotence=true` do Kafka producer para exactly-once | Não cobre falha de broker/consumer; precisa de `foreachBatch` no consumer |
| `dropDuplicates()` no consumer streaming | Estado infinito sem TTL — vaza memória |
| Ler watermark e descartar duplicatas pelo `id` | Funciona até a janela passar; depois, idempotência falha |

## Observabilidade da idempotência

Métricas e checks que validam a estratégia em runtime:

| Métrica/Check | Onde | O que protege |
|---|---|---|
| `pipeline_records_written_total{mode}` Prometheus | Pipelines batch | Detecta volume anômalo (reexecução escondida) |
| `merge_inserted_count`, `merge_updated_count` | Logs Spark | Auditoria de MERGE |
| Smoke test: rodar DAG 2x → row count Bronze idêntico | CI | Regressão de idempotência |
| Smoke test: emitir mesmo evento 2x no Kafka → 1 linha em Bronze | CI | Validação foreachBatch |
| Tabela `pipeline_runs` no DW | Metadata | `(dag_id, run_id, partition, status, written_at)` por execução |

## Consequências

### Positivas

- **Reexecução previsível** — é possível pedir "rode duas vezes" e ver o mesmo resultado
- **Backfill seguro** — janela histórica reprocessável sem caos
- **Resposta técnica** para perguntas "e se Kafka reentregar?" e "e se DAG falhar no meio?"
- **Otimização inerente** — MERGE Delta evita reescrever quando nada mudou (`_row_hash`)

### Negativas (e mitigações)

- **Custo de MERGE** maior que append: mitigado por `_row_hash` short-circuit + Z-ORDER por chave
- **Partição mal escolhida** pode forçar reescrita ampla: documentar partições em `data-model.md`; revisar antes de mudar
- **`foreachBatch` adiciona complexidade ao consumer streaming**: aceitar; é o preço do exactly-once

## Referências

- **Delta Lake MERGE docs:** <https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge>
- **Spark Structured Streaming exactly-once:** <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics>
- **Kafka idempotent producer:** <https://kafka.apache.org/documentation/#idempotentproducer>
- **ADR 0001 Lambda vs Kappa** (define batch vs streaming) — [`./0001-lambda-vs-kappa.md`](./0001-lambda-vs-kappa.md)
- **ADR 0006 SCD Tipo 2** (consumidor desta política para dimensões) — [`./0006-scd2.md`](./0006-scd2.md)

## Apêndice — FAQ técnico

| Pergunta | Resposta de 30s |
|---|---|
| Se o DAG rodar 2x, duplica? | Não. Bronze faz `INSERT OVERWRITE PARTITION`; mesma execução produz mesmo resultado. |
| Como você garante exactly-once com Kafka? | Producer idempotente + `foreachBatch` MERGE no Spark Streaming, com chave `(id_paciente, ts_evento)`. |
| Backfill 5 anos é seguro? | Sim — cada partição mensal é independente; rodam em paralelo via Airflow `max_active_runs` controlado. |
| O MERGE no Silver é caro? | Mitigado por `_row_hash` que detecta mudança antes de rewrite — quase tudo vira NO-OP em rerun limpo. |
| E se o Kafka cair durante a demo? | Producer faz buffer e retry; consumer reinicia do checkpoint S3, sem perda nem duplicação. |

---

# ADR 0003 — Política de Schema Evolution

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza

## Contexto

Fontes de dados externas mudam:

- **DataSUS** já mudou o layout do SIH-SUS pelo menos 3 vezes em 20 anos (acréscimo de colunas como `ETNIA`, `RACA_COR`)
- **CNES API** adicionou `tem_uti` em 2022 — campo novo entrou silenciosamente na resposta JSON
- **IBGE Sidra** mantém compatibilidade, mas ocasionalmente publica novo agregado que substitui antigo

Sem política, o pipeline quebra a cada mudança upstream e o backfill histórico fica refém. A Pergunta relevante::

> *"E se um arquivo do DataSUS chegar com schema diferente — uma coluna nova ou um tipo mudado?"*

Esta decisão estabelece **como cada camada lida com mudanças** e **quem é responsável** por aprovar uma mudança breaking.

## Princípios

1. **Bronze é tolerante.** Aceita colunas novas automaticamente; preserva o que veio sem julgamento.
2. **Silver é estrito.** Schema explícito declarado em código; mudança requer alteração consciente do contrato.
3. **Gold é versionado.** Mudanças passam por migrations (estilo Alembic/dbt seeds).
4. **Quebra de contrato é evento de gerenciamento**, não acidente — exige PR + revisão + plano de reprocessamento.
5. **Bronze nunca remove colunas** — mesmo que upstream pare de enviar, manter `NULL` para preservar histórico.

## Política por camada

### Bronze — `mergeSchema=true` + log de drift

**Comportamento:**
- Spark grava com `option("mergeSchema", "true")` em Delta
- Coluna nova na fonte → adicionada ao schema Bronze automaticamente; valor nas linhas antigas é NULL
- Coluna sumiu na fonte → continua existindo no Bronze como NULL para novas linhas
- Tipo conflitante (string virou int) → **falha intencional** (não tentar coerção silenciosa)

**Responsabilidade:**
- Pipeline registra **drift detectado** em métrica `bronze_schema_drift_count{table}` no Prometheus
- Alerta soft: ">0 em 24h" notifica analista para revisar
- Smoke test inclui assert de "schema do dia bate com schema do dia anterior" → quebra é **warning**, não erro fatal

**Exemplo concreto (CNES):**
- Mês X: API retorna `{codigo_cnes, nome, ...}`
- Mês X+1: API retorna `{codigo_cnes, nome, novo_campo, ...}`
- Bronze: aceita silenciosamente, `novo_campo` populado para mês X+1, NULL para mês X

### Silver — schema explícito em `pipelines/common/schemas.py`

**Comportamento:**
- Cada tabela Silver tem `StructType` declarado em código
- Leitura do Bronze usa `schema=SilverXxxSchema` — colunas inesperadas no Bronze são **ignoradas** (não sobem para Silver sem ato consciente)
- Coluna esperada faltando no Bronze → **falha o job** com mensagem clara
- Tipo divergente → **falha**

**Responsabilidade:**
- Mudança em Silver é **PR review obrigatório**
- Mudança breaking (remover coluna, mudar tipo) requer:
  - ADR específico ou seção em ADR existente
  - Plano de reprocessamento documentado no PR
  - Migration script (mesmo que seja apenas SQL `ALTER TABLE`)

**Exemplo concreto:**
- Decisão: incluir `etnia` no Silver `fato_internacao`
- Passos: (1) atualizar `SilverInternacaoSchema`, (2) atualizar transformação, (3) backfill Silver desde 2018, (4) Gold consequente

### Gold — versionamento explícito + migration

**Comportamento:**
- Schema Gold descrito em SQL DDL versionado
- Migrations rastreadas — ferramenta a definir, candidatos: dbt seeds + snapshots, Alembic, ou scripts numerados em `infra/postgres/migrations/`
- Mudança requer migration **forward-compatible** quando possível (adicionar coluna NULLABLE com default)
- Migrations executadas pelo Airflow via DAG dedicado antes de qualquer carga downstream

**Compatibilidade Metabase / Trino:**
- Adicionar coluna no Gold → dashboards continuam funcionando
- Renomear coluna no Gold → quebra dashboards; **fazer via "add new + deprecate old"**: criar coluna nova, popular ambas em paralelo por 2 ciclos, depois remover antiga

## Tipos de mudança e estratégia

| Tipo de mudança | Bronze | Silver | Gold |
|---|---|---|---|
| **Adicionar coluna** (novo campo opcional) | ✅ silencioso (mergeSchema) | PR + atualização do `StructType` | Migration ALTER TABLE ADD COLUMN |
| **Remover coluna upstream** | mantém como NULL para novos registros | Decisão consciente: ignorar ou marcar deprecated | Manter coluna no Gold (NULL); remover só após plano completo |
| **Renomear coluna upstream** | ⚠️ quebra silencioso — coluna nova entra, antiga vira NULL | Mapeamento explícito no transform Bronze→Silver | Não exposta no Gold (mapeamento para nome canônico em Silver) |
| **Mudança de tipo** (string→int) | ❌ falha intencional | N/A — pega no Bronze | N/A |
| **Mudança semântica** (mesma coluna, novo significado) | Não detectável automaticamente | Reanalisar transform; possível breaking | Possível migration |

## Schema Registry (decisão deliberada de **NÃO** usar)

Avaliado: usar Confluent Schema Registry para o stream Kafka.

**Por que rejeitado neste case:**
- Adiciona um container ao Compose (custo de RAM e debug)
- Para um único producer/consumer interno, é overkill
- Validação client-side com Pydantic cobre o caso (vide [`stream-faker.md`](../../integrations/stream-faker.md))

**Quando incluir (evolução):** múltiplos producers/consumers, contratos entre times, evolução Avro com regras de compat backward/forward — aí Schema Registry passa a valer.

## Quebra explícita — "schema-breaking change"

Quando inevitável (ex.: DataSUS muda significado de `MORTE` de '0/1' para 'S/N'):

1. **Detectar:** smoke test ou drift alert dispara
2. **Pausar pipeline** afetado (Airflow `paused`)
3. **Decidir:** ADR específico documentando a mudança e o plano
4. **Implementar transform de compatibilidade** se possível ('S'→'1', 'N'→'0' no Silver)
5. **Backfill** reprocessando dados afetados
6. **Reativar** pipeline
7. **Documentar** em CHANGELOG do projeto

## Versão da camada de transformação

A versão do código de transformação fica em coluna metadata:
- `_silver_transform_version` (string, ex.: `"silver-1.2.3"`) em cada linha do Silver
- `_gold_transform_version` em cada linha do Gold

Permite identificar linhas processadas com versão antiga após upgrade. Reprocessamento dirigido: `WHERE _silver_transform_version < '1.3.0'`.

## Casos especiais

### Stream Faker (controlamos o producer)

- Schema do payload tem `schema_version: int`
- Producer incrementa em **mudança breaking**
- Consumer Spark filtra por `schema_version` conhecida
- Versão desconhecida → DLQ + alerta

### CNES API (não controlamos)

- Bronze aceita drift silencioso
- Snapshot diário permite comparar dia atual vs anterior — diff de schema é exibido em log
- PR review do Silver confronta com a API e decide promover novos campos

### DataSUS CSV (estável mas evolui)

- Versão antiga do RD tem ~120 colunas; versão atual tem ~140
- Bronze aceita ambas via `mergeSchema=true`
- Silver promove apenas as 19 colunas canonicalizadas — colunas extras ficam dormentes no Bronze para futura promoção

## FAQ Técnico

| Pergunta | Resposta de 30s |
|---|---|
| E se uma coluna nova aparecer no DataSUS? | Bronze aceita via `mergeSchema=true`, registra drift no Prometheus. Decisão de promover ao Silver é PR-review com ADR explícito. |
| E se uma coluna sumir? | Bronze mantém NULL para novos registros — histórico preservado. Silver continua exigindo a coluna; se não chegar, falha intencional para forçar revisão. |
| E se o tipo mudar (string virou int)? | Bronze **falha** intencionalmente. Coerção silenciosa esconde bug. |
| Como migrar o Gold? | Migrations versionadas executadas por DAG dedicado antes da carga. Mudanças preferem ser forward-compatible (ADD COLUMN NULL DEFAULT). |
| Por que não Schema Registry? | Overkill para 1 producer/consumer interno. Pydantic cobre. Mostro no slide como evolução para múltiplos times. |

## Consequências

### Positivas

- Mudança incremental no upstream **não quebra** o pipeline
- Mudança breaking é **detectada e gerenciada**, não ignorada
- Histórico Bronze preserva tudo — backfill com schema novo sempre possível
- Versão da transformação rastreável linha-a-linha

### Negativas

- Bronze pode crescer com colunas dormentes que nunca chegam ao Silver (mitigado pelo VACUUM e pela natureza colunar do Parquet — colunas vazias custam quase zero)
- Discordância entre Bronze e Silver pode confundir analistas (mitigado por documentação clara: "Silver é o contrato")

## Referências

- **Delta Lake schema evolution:** <https://docs.delta.io/latest/delta-batch.html#schema-validation>
- **Iceberg schema evolution (comparativo):** <https://iceberg.apache.org/docs/latest/evolution/>
- **Confluent Schema Registry:** <https://docs.confluent.io/platform/current/schema-registry/index.html>
- **Pydantic for runtime validation:** <https://docs.pydantic.dev/latest/>
- **dbt snapshots/seeds:** <https://docs.getdbt.com/docs/build/snapshots>

---

# ADR 0004 — Estratégia de Mascaramento de PII

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza
- **Contexto do case:** Academia Santander — Engenharia de Dados

## Contexto

O enunciado é literal sobre dois requisitos cruzados:

> **Requisito 5 (Segurança de Dados):** "Garanta a segurança e confidencialidade dos dados sensíveis dos pacientes, adotando práticas de criptografia, controle de acesso e cumprindo regulamentações de proteção de dados, como a Lei Geral de Proteção de Dados (LGPD)."
>
> **Requisito 6 (Mascaramento de Dados):** "Proponha técnicas e exemplos de como mascarar dados sensíveis, garantindo a privacidade e anonimização das informações utilizadas na análise."

A camada `oltp.paciente` traz **PII verdadeiro** (CPF com checksum válido, nome, CEP, telefone, e-mail) gerado via Faker `pt_BR`. O Bronze contém esses dados em claro — propositadamente, para demonstrar a transição "raw → seguro" durante a apresentação. **A partir do Silver, ninguém pode mais ver PII em claro.**

Esta decisão registra **onde, como e por que** o mascaramento acontece, **quais técnicas** se aplicam a cada coluna sensível e **quais alternativas** foram avaliadas e rejeitadas.

## Opções avaliadas

### Opção A — UDFs PySpark aplicadas no Bronze→Silver ✅ (escolhida)

Funções puras em `pipelines/common/masking.py` aplicadas como UDFs durante a transformação Bronze→Silver. A mesma biblioteca é importada por:
- DAGs de batch (Airflow + Spark) para mascarar `oltp.paciente`
- Spark Structured Streaming para mascarar `atendimentos_stream` em tempo real
- Testes unitários (`tests/unit/test_masking.py`)

**Vantagens:**
- **Mascaramento aplicado uma única vez**, no ponto exato em que o dado deixa a camada raw
- Lógica versionada em código, testável, reproduzível
- Mesma função usada por batch e streaming → **invariante: dado em Silver+ NUNCA tem PII**
- Auditável: o `git blame` mostra quando e por quem cada técnica foi alterada
- Desacopla armazenamento (Delta) da política — qualquer migração futura preserva o tratamento

**Desvantagens (mitigadas):**
- UDFs Python têm overhead vs operações nativas → **mitigação:** preferir SQL Spark nativo (`sha2`, `concat`, `substring`) sempre que possível; UDF Python só onde inevitável (Faker)
- Salt em variável de ambiente é menos seguro que cofre — **mitigação:** documentado em [`security.md`](../../security.md) como evolução para Vault em produção

### Opção B — Views Postgres com funções PL/pgSQL — rejeitada

Criar `oltp.paciente_anonimo` como view que aplica `digest()` etc. e expor apenas a view para extração.

**Por que foi rejeitada:**
- Limita o mascaramento ao dado vindo de Postgres — precisaríamos de outra solução para o stream Kafka, que não passa por Postgres
- Lógica de mascaramento espalhada (PL/pgSQL + Python) → divergência inevitável
- Funções PL/pgSQL são chatas de testar
- Quebra o princípio de "mascaramento centralizado em código"

### Opção C — Column-level encryption no Delta Lake — rejeitada

Delta Lake suporta criptografia de colunas via Parquet Modular Encryption (PME). Cada coluna tem chave própria.

**Por que foi rejeitada:**
- PME é complexo: KMS, key rotation, política de acesso por footer
- **Reversível por design** (criptografia, não anonimização) → não atende ao requisito de **anonimização** literal do enunciado
- Footprint operacional alto para o ganho — pode ser considerado como como evolução, mas não como ponto de partida
- Em laptop com MinIO sem KMS real, vira teatro

### Opção D — Trino views com `mask_fn` — rejeitada

Trino Lake Formation policies aplicam mascaramento na hora da query.

**Por que foi rejeitada:**
- Mascaramento no momento da consulta deixa o dado **em claro** no storage (Silver) — quem tem acesso direto ao MinIO bypassa a política
- O case quer dado anonimizado **persistido**, não só visualizado anonimamente
- Útil em produção como **camada extra** sobre dado já anonimizado, não como única proteção

## Decisão

**Adotar UDFs PySpark em `pipelines/common/masking.py`** aplicadas exclusivamente na transição Bronze → Silver, com as seguintes técnicas por coluna:

| Coluna PII | Técnica | Reversível? | Determinístico? | Justificativa |
|---|---|---|---|---|
| `cpf` | **Hash SHA-256 + salt** | Não | Sim | Preserva join (mesmo CPF → mesmo hash) sem rastrear identidade |
| `nome` | **Faker pt_BR com seed = hash(cpf)** | Não | Sim | Aparência realista para análise; mesmo paciente → mesmo nome fake |
| `data_nascimento` | **Generalização: ano apenas** (`yyyy`) | Parcial | Sim | Habilita faixa etária; remove identificação fina |
| `cep` | **Truncamento: 3 primeiros dígitos** | Parcial | Sim | Análise por microrregião; perde nível de quadra |
| `email` | **Supressão (NULL)** | — | — | Sem valor analítico que justifique guardar |
| `telefone` | **Supressão (NULL)** | — | — | Idem |
| `endereco_completo` (não está no schema atual mas se entrar) | **Supressão** | — | — | Idem |

### Algoritmo de hash (FAQ técnico)

```
hash_cpf = SHA-256( salt || cpf_normalizado )
salt     = variável de ambiente PII_HASH_SALT (256+ bits)
```

- **SHA-256** é escolhido por estar entre os algoritmos sugeridos pela ANPD para anonimização (junto com SHA-3 e BLAKE2)
- **Salt fixo por ambiente** (não por linha) — necessário para preservar o **determinismo do join** (`fato_internacao.sk_paciente` resolve para `dim_paciente.sk_paciente` via lookup pelo hash). Salt aleatório por linha quebraria o join e tornaria o pipeline inútil.
- **Salt por linha** seria pseudonimização forte mas inviabilizaria o caso de uso analítico — explicitamente fora do escopo

### Determinismo de Faker

```
seed = int(hash_cpf[:16], 16)   # primeiros 64 bits do hash como int
fake = Faker(locale='pt_BR')
Faker.seed(seed)
nome_fake = fake.name()
```

Mesmo CPF original → mesmo nome Faker em todas as execuções. Diferentes CPFs → nomes diferentes (com colisões raríssimas inerentes ao birthday paradox em Faker, ~0.001% para 50k pacientes — aceitável para análise).

## Consequências

### Positivas

- **Conformidade LGPD:** SHA-256 + salt é considerado anonimização aceitável pela ANPD desde que o salt não seja exposto e a chave da função hash seja gerenciada
- **Respostas para 4 perguntas técnicas frequentes:**
  - "Como vocês mascaram CPF?" → hash com salt secreto
  - "E se alguém tiver acesso aos dados mascarados, consegue identificar a pessoa?" → não, porque o salt é segredo e SHA-256 é unidirecional
  - "Por que SHA-256 e não MD5?" → MD5 está formalmente quebrado para colisões; SHA-256 é o mínimo industrial
  - "E se você quiser saber quantas internações um paciente teve?" → contagem é trivial: `COUNT(*) GROUP BY hash_cpf`
- **Reuso em batch e streaming** garante invariante "PII some no Silver"
- **Testável** com fixtures conhecidas (CPF do exemplo da Receita: `123.456.789-09` → hash determinístico)

### Negativas (e mitigações)

- **Timing attacks no hash:** PHP-style attacks comparando tempo de hash não se aplicam (não é login). Ignorável.
- **Salt na env:** menos seguro que Vault. Mitigação: `.env` no `.gitignore` desde Dia 1; runbook documenta rotação; produção usa Vault/Secrets Manager (slide).
- **Rotação de salt invalida joins históricos:** mudar o salt requer **reprocessamento completo do Silver e Gold**. Mitigação: política documentada de rotação anual com janela de manutenção; histórico antes da rotação fica em snapshot offline.
- **Faker pt_BR pode gerar nome real por coincidência:** birthday paradox em catálogo finito de Faker → ~0.5% de chance em 50k pacientes. Mitigação: aceitar probabilidade (estatisticamente seguro, e o nome não é ligável ao CPF original).
- **Generalização parcial não é anonimização forte:** mesmo o CEP de 3 dígitos + ano de nascimento + sexo pode permitir reidentificação em municípios pequenos. Mitigação documentada: aplicar **k-anonymity adicional no Gold** se análises por município pequeno forem necessárias (mover para SK 5+).

## Pontos de aplicação no pipeline

```
[Bronze]                    [pipelines/common/masking.py]              [Silver]
oltp.paciente   ──→   apply_paciente_masking(df)   ──→   silver.paciente_mascarado
                          │
                          ├── hash_cpf()
                          ├── fake_name(seed=hash_cpf)
                          ├── generalize_birth_year()
                          ├── truncate_cep_3()
                          ├── suppress(email, telefone)
                          └── add_meta_columns(masked_at, masking_version)


atendimentos_stream  ──→   apply_stream_masking(df)   ──→   silver.atendimentos
   (Bronze append)              │
                                ├── hash_cpf()  (mesmo salt → joina com dim_paciente)
                                └── add_meta_columns(...)
```

## Rastreabilidade e versionamento

Toda linha mascarada em Silver carrega:
- `masked_at TIMESTAMPTZ` — quando foi mascarado
- `masking_version STRING` — versão do `masking.py` que rodou (constante exportada do módulo, ex.: `"masking-1.0.0"`)

Mudança de técnica de mascaramento = nova versão = novo reprocessamento. **Linhas com versões diferentes coexistem temporariamente** durante migração; relatório de drift detecta.

## Auditoria

- **Eventos OpenLineage** emitidos em cada job de mascaramento incluem nome do dataset de entrada (Bronze) e saída (Silver), permitindo rastrear o lineage completo no Marquez
- **Métrica Prometheus** `masking_records_total{technique="hash_cpf"}` permite alertar se queda brusca (sintoma de bug que deixa PII passar)
- **Smoke test** valida que `silver.paciente_mascarado.cpf` não tem nenhum CPF que apareça no Bronze original (proteção contra regressão)

## Referências

- **ANPD — Guia de Boas Práticas LGPD:** <https://www.gov.br/anpd/pt-br/documentos-e-publicacoes/guia-de-boas-praticas-lgpd>
- **ANPD — Estudo Técnico sobre Pseudonimização e Anonimização:** <https://www.gov.br/anpd/pt-br/documentos-e-publicacoes/2024-temp/estudo-tecnico-anonimizacao>
- **NIST SP 800-188 — Trustworthy De-Identification Techniques** (referência internacional comparativa)
- **OWASP — Hashing Cheat Sheet:** <https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html> (sobre SHA-256 vs alternativas)
- **k-anonymity (Sweeney, 2002):** referência clássica para reidentificação por quasi-identifiers
- **Decisão relacionada:** [Modelo dimensional Gold](../data-model.md) define que dimensões só recebem dados pós-masking

## Apêndice — perguntas técnicas + resposta

| Pergunta provável | Resposta de 30s |
|---|---|
| Por que hash em vez de criptografia? | Hash é unidirecional — anonimiza. Criptografia é reversível — pseudonimiza. LGPD pede anonimização para dados que não precisam voltar. |
| Por que SHA-256 e não SHA-3? | Por velocidade e suporte de plataforma. Spark `sha2(col, 256)` é nativo e batch-friendly. SHA-3 é equivalente em segurança mas menos suportado no ecossistema atual. |
| Como você protege o salt? | Em dev: `.env` no `.gitignore`, rotação documentada. Em prod: Vault + chave em KMS. Acesso ao salt é o mesmo nível de acesso que ao Bronze. |
| Mascarar quebra o join entre fato e dimensão? | Não — o hash é determinístico, então mesmo CPF resulta sempre no mesmo hash, e joins funcionam. |
| Reidentificação por quasi-identifiers (CEP+sexo+ano)? | Risco real. Mitigamos com **generalização** (CEP 3 dígitos, ano apenas) e documentamos `k-anonymity` como evolução para análises municipais finas. |
| E se um paciente exercer "direito ao esquecimento" da LGPD? | Documentado em [`security.md`](../../security.md): processo de localização do hash via salt + DELETE no Bronze + reprocessamento Silver/Gold. Política de retenção do Bronze: 7 dias após mascaramento. |

---

# ADR 0005 — Watermark e Tratamento de Eventos Atrasados (Streaming)

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza

## Contexto

A camada **speed** da arquitetura Lambda processa eventos de atendimentos publicados em Kafka pelo `stream-producer` simulado ([`stream-faker.md`](../../integrations/stream-faker.md)). Em sistemas reais, esses eventos chegam **fora de ordem** — origens espalhadas, redes intermitentes, retries do producer, replays operacionais. Pergunta técnica relevante:

> *"O que acontece se um evento chegar 30 minutos atrasado? E 5 horas? E 3 dias?"*

Esta decisão registra:
- **Watermark** — quanto de atraso aceitamos
- **Trigger interval** — frequência do micro-batch
- **DLQ** — onde param eventos rejeitados
- **Exactly-once** — como garantimos
- **Checkpoint** — onde guardar progresso

## Princípios

1. **Event time é a verdade**, não processing time. Janelas e watermark sempre sobre `ts_evento` do payload.
2. **Watermark generoso o suficiente** para tolerar falhas humanas razoáveis, **conservador o suficiente** para não inflar estado.
3. **Eventos rejeitados não somem** — vão para DLQ com motivo.
4. **Restart é seguro** — checkpoint persistente em S3 garante.
5. **Idempotência via MERGE no `foreachBatch`** — vide [ADR 0002](./0002-idempotency-merge.md).

## Decisão

### Watermark = **1 hora**

Eventos com `ts_evento` mais antigo que `max(ts_evento_observado_até_agora) - 1h` são **rejeitados** (logados em DLQ).

**Por que 1h:**
- Cobre 99% dos cenários reais de atraso (retry, restart de producer, falha curta de rede)
- Estado do Spark Streaming (para join com janela) cresce proporcional ao watermark — 1h cabe em laptop
- Atraso > 1h tipicamente indica **problema operacional sério** (producer parado por horas, replay manual) — preferimos tratar via reprocessamento batch que via streaming
- Demonstrável na demo: emitir evento com `ts_evento - 30min` ainda é aceito; com `ts_evento - 2h`, vai pra DLQ

**Quando reconsiderar:** em produção real em produção real o watermark é **negociado com o time produtor** (SLA de pontualidade).

### Trigger interval = **30 segundos** (processing time)

Micro-batches a cada 30s.

**Por que 30s:**
- Responsivo na demo — gráfico no Grafana mostra movimento sem ficar parado
- Não martela CPU como `continuous=true` ou trigger `0s`
- Suficiente para volumes da demo (5–50 eventos/seg → 150–1500 eventos por batch)
- Em produção, ajustável por throughput (1s a 1min é faixa típica)

### Trigger alternativo `availableNow=True`

Para CI e smoke test, usar `Trigger.AvailableNow()` em vez de processing-time. Isso:
- Lê **todos os offsets disponíveis** no Kafka até o momento
- Encerra a query (não fica rodando)
- CI fica determinístico: smoke test produz 100 eventos, consumer processa todos, finaliza

### DLQ — tópico Kafka `atendimentos.dlq`

Eventos rejeitados vão para tópico Kafka separado com payload original + motivo.

**Motivos previstos:**
| Motivo | Quando |
|---|---|
| `LATE_EVENT` | `ts_evento` < watermark atual |
| `SCHEMA_INVALID` | Falhou Pydantic / StructType |
| `UNKNOWN_PATIENT` | `id_paciente` não existe no OLTP (FK órfão) |
| `UNKNOWN_VERSION` | `schema_version` desconhecido |
| `MASKING_FAILED` | UDF de mascaramento lançou exceção |

**Schema do payload DLQ:**

```
{
  "original_payload": <JSON original como string>,
  "kafka_topic": "atendimentos.raw",
  "kafka_partition": 1,
  "kafka_offset": 12345,
  "error_reason": "LATE_EVENT",
  "error_detail": "ts_evento=2026-05-21T08:00 < watermark=2026-05-21T13:30",
  "rejected_at": "2026-05-21T14:30:00Z"
}
```

DLQ é **persistido em Bronze** via DAG batch separado (executa de hora em hora) → permite análise posterior e reprocessamento manual.

### Checkpoint location

```
s3a://landing/_checkpoints/atendimentos_stream_consumer/
```

- Spark Structured Streaming requer `checkpointLocation` para retomar de onde parou após restart
- **Cada query Streaming tem seu próprio checkpoint** — não compartilhar entre queries
- Backup do checkpoint a cada 1h (DAG batch) — protege contra corrupção do bucket

### Exactly-once via `foreachBatch` + Delta MERGE

```
def write_to_bronze(microbatch_df, batch_id):
    (microbatch_df
       .alias("src")
       .join(...)  # eventual enrich
       .write
       .format("delta")
       .mode("append")  # ou usar deltaTable.merge() para dedupe
    )

# Mais robusto: MERGE explícito
def write_to_bronze_with_merge(microbatch_df, batch_id):
    DeltaTable.forPath("s3a://bronze/atendimentos_stream") \
      .alias("tgt") \
      .merge(microbatch_df.alias("src"),
             "tgt.id_paciente = src.id_paciente AND tgt.ts_evento = src.ts_evento") \
      .whenNotMatchedInsertAll() \
      .execute()
```

> **Atenção:** o trecho acima é **pseudo-código** ilustrativo. A implementação real fica em `pipelines/streaming/consumer.py` (não escrita pela IA).

## Configurações Spark Streaming consolidadas

| Configuração | Valor | Razão |
|---|---|---|
| `kafka.bootstrap.servers` | `kafka:9092` | Endereço interno |
| `subscribe` | `atendimentos.raw` | Tópico fonte |
| `startingOffsets` | `earliest` (CI) / `latest` (demo) | Em CI quero o histórico de teste; em demo, só novos |
| `failOnDataLoss` | `false` | Permite recuperação de erro de offsets |
| `maxOffsetsPerTrigger` | `1000` | Backpressure — evita engasgo em burst |
| `kafka.session.timeout.ms` | `30000` | Default OK |
| `trigger` | `processingTime='30 seconds'` | Vide acima |
| `outputMode` | `append` (com MERGE no foreachBatch) | Apropriado para fato |
| `withWatermark("ts_evento", "1 hour")` | aplicado no DataFrame antes da agregação | — |
| `checkpointLocation` | `s3a://landing/_checkpoints/...` | Persistente |

## Janelas de agregação (caso queira métrica em tempo real)

Para alimentar uma tabela `gold.fato_atendimento_stream_agg_5min`:

```
events
  .withWatermark("ts_evento", "1 hour")
  .groupBy(window("ts_evento", "5 minutes"), "cnes_estabelecimento")
  .count()
```

Janela tumbling de 5min, agrupada por estabelecimento. Resultado: contagem de atendimentos por hospital a cada 5min, atualizada a cada trigger.

**Implementação fica fora deste ADR**; aqui só o conceito.

## Restart e recuperação

Cenários e comportamento:

| Cenário | Comportamento |
|---|---|
| Container Spark reiniciou | Lê `checkpointLocation`, retoma do último offset commit. Sem perda. |
| Bucket MinIO temporariamente fora | Spark falha o batch, faz retry exponencial (3 tentativas). Após esgotar, query falha — Airflow `airflow-streaming-supervisor` (S4) detecta e reinicia. |
| Kafka tópico foi recriado (offsets perdidos) | `failOnDataLoss=false` permite reler do `earliest`; pode haver duplicação que o MERGE deduplica. |
| Watermark zerou (estado perdido) | Eventos antigos rejeitados como late até o watermark "convergir" para o presente — comportamento aceitável. |
| Schema do tópico mudou (Faker bug) | DLQ pega; alerta dispara em <30s. |

## Consequências

### Positivas

- Garantia **exactly-once efetivo** validável em smoke test
- DLQ visível e analisável — resposta para "como você lida com dados ruins?"
- Restart automático graças ao checkpoint
- Watermark **demonstrável ao vivo\*\*: emitir um evento atrasado e mostrar ele caindo na DLQ
- Métricas Prometheus (`stream_dlq_count`, `stream_processing_lag`) cobrem requisito 4 (observabilidade)

### Negativas (e mitigações)

- **Estado de 1h** consome RAM proporcional ao throughput → cap de RAM no spark-worker; em produção, ajustar
- **`failOnDataLoss=false`** pode esconder problemas reais — mitigação: alerta em `kafka_data_loss_total`
- **Trigger 30s** introduz latência mínima de 30s — aceitável para o caso (saúde pública não exige tempo real <1s)
- **DLQ infinita** se ninguém olhar — política: reprocessar/expirar mensalmente

## FAQ Técnico

| Pergunta | Resposta de 30s |
|---|---|
| Por que watermark de 1h? | Cobre 99% dos atrasos reais sem inflar estado. Configurável. Atraso maior é tratado via reprocessamento batch. |
| O que acontece com evento atrasado? | Vai para `atendimentos.dlq` com motivo `LATE_EVENT`. Alerta no Grafana. Análise/reprocessamento manual. |
| Exactly-once é possível? | Efetivamente sim — producer idempotente + checkpoint Spark + MERGE Delta no foreachBatch. Garantia ponta-a-ponta. |
| Por que processing-time 30s e não continuous? | Trade-off responsividade × CPU. Continuous tem latência menor mas é experimental e pesa em laptop. 30s é ótimo para o caso. |
| E se o broker Kafka cair? | Producer faz buffer + retry; consumer faz retry exponencial. Após 3 falhas, supervisor (Airflow) reinicia a query. |
| Como você gerencia o estado da janela de agregação? | Watermark expira chaves antigas automaticamente. Sem TTL, estado vazaria. Default Spark Streaming é correto desde que watermark esteja declarado. |

## Referências

- **Spark Structured Streaming Programming Guide:** <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>
- **Watermarking:** <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking>
- **`foreachBatch`:** <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch>
- **Delta Lake streaming:** <https://docs.delta.io/latest/delta-streaming.html>
- **Tyler Akidau — "Streaming 101 / 102"** (referência conceitual): <https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/>
- **ADR 0002 idempotência** — [`./0002-idempotency-merge.md`](./0002-idempotency-merge.md)
- **ADR 0001 Lambda** — [`./0001-lambda-vs-kappa.md`](./0001-lambda-vs-kappa.md)

---

# ADR 0006 — Implementação de SCD Tipo 2 nas Dimensões

- **Status:** Aceito
- **Data:** 2026-05-07
- **Autor:** Matheus Fideles Martins de Souza

## Contexto

O modelo dimensional Gold (definido em [`data-model.md`](../data-model.md)) usa **SCD Tipo 2** em duas dimensões:
- `dim_paciente` — atributos como CEP de residência mudam quando o paciente muda de cidade
- `dim_estabelecimento` — atributos como `tem_uti` e `leitos_existentes` mudam quando o hospital expande

Análises sérias **dependem do contexto da época** do fato:
- Quantas internações ocorreram quando o hospital ainda **não tinha UTI**?
- Onde a paciente residia quando foi internada (não onde reside hoje)?

Sem SCD2, **toda análise histórica fica errada** — o lookup retorna o estado atual da dimensão, não o estado vigente na data do fato.

A Pergunta relevante::

> *"Como vocês tratam mudanças nas dimensões? Se um paciente mudar de endereço, o que acontece com as internações antigas dele?"*

Esta decisão registra a estratégia de implementação.

## Tipos de SCD considerados

### Tipo 0 — Imutável

Não muda jamais. Útil para CID-10, códigos de procedimento (mudanças são raríssimas e os códigos antigos coexistem com novos).

**Aplicado em:** `dim_cid`, `dim_procedimento`, `dim_tempo`.

### Tipo 1 — Sobrescrita

Atualiza atributo, perde histórico.

**Quando aceitável:** atributo cuja história não tem valor analítico (ex.: nome do município mudou de "Embu" para "Embu das Artes").

**Aplicado em:** `dim_municipio` (nome, região; código IBGE imutável).

### Tipo 2 — Linha versionada ✅ (escolhida para paciente e estabelecimento)

Cada mudança vira nova linha; antiga é "fechada" com `dt_fim`.

| sk | nk_cnes | nome | tem_uti | leitos | dt_inicio | dt_fim | is_current |
|---|---|---|---|---|---|---|---|
| 200 | 2077426 | HC FMUSP | false | 800 | 2018-01-01 | 2020-06-30 | false |
| 201 | 2077426 | HC FMUSP | true | 1200 | 2020-07-01 | 2024-12-31 | false |
| 202 | 2077426 | HC FMUSP | true | 2400 | 2025-01-01 | 9999-12-31 | true |

Fato resolve `sk_estabelecimento` pelo lookup com `event_date BETWEEN dt_inicio AND dt_fim`.

**Vantagem:** histórico preservado.
**Desvantagem:** consultas precisam aprender o lookup; tabela cresce.

### Tipo 3 — Coluna anterior

Mantém apenas a versão imediatamente anterior em coluna separada.

**Por que rejeitado:** suporta apenas 1 nível de histórico. Insuficiente para casos com mudanças múltiplas.

### Tipo 4 — Tabela de histórico separada

Tabela atual + tabela de histórico.

**Por que rejeitado:** dobra o número de tabelas e de queries; modelo Tipo 2 cobre o mesmo caso de uso com modelagem mais limpa.

### Tipo 6 — Híbrido (1 + 2 + 3)

Combina os três: linha versionada + coluna "atual" sempre populada para acesso rápido + coluna anterior.

**Por que rejeitado para o case:** complexidade desproporcional ao volume; SCD2 puro com `is_current` flag já dá acesso "atual" rápido (`WHERE is_current=true`).

## Decisão

**Adotar SCD Tipo 2 puro com flag `is_current`** em `dim_paciente` e `dim_estabelecimento`. SCD Tipo 1 em `dim_municipio` (atributos descritivos). SCD Tipo 0 em `dim_cid`, `dim_procedimento`, `dim_tempo`.

## Schema das colunas SCD2 (padrão para todas as dimensões SCD2)

```
sk_<nome>            BIGINT     -- surrogate key, cresce a cada versão
nk_<chave_natural>   STRING     -- preservada (ex.: nk_cpf_hash, nk_cnes)
... colunas de atributos ...
dt_inicio            TIMESTAMP  -- quando esta versão começou a valer
dt_fim               TIMESTAMP  -- quando esta versão deixou de valer (9999-12-31 se atual)
is_current           BOOLEAN    -- true se esta é a versão atual
_attr_hash           STRING     -- hash MD5/SHA-256 dos atributos (detecção de mudança)
_loaded_at           TIMESTAMP  -- meta: quando esta linha foi inserida
```

## Padrão de carga (semântica, não código)

A transformação Silver→Gold para cada dimensão SCD2 segue este padrão:

```
1. Ler snapshot atual do Silver (linhas com nk_* e atributos)
2. Calcular _attr_hash de cada linha (hash dos atributos relevantes)
3. Para cada nk_* no snapshot:
   a) Buscar versão CURRENT na dim Gold (is_current=true)
   b) Se não existe → INSERT nova linha com dt_inicio=event_ts, dt_fim=9999-12-31, is_current=true
   c) Se existe e _attr_hash igual → NO-OP (nada mudou)
   d) Se existe e _attr_hash diferente → UPDATE versão antiga (dt_fim=event_ts, is_current=false)
                                         + INSERT nova linha (dt_inicio=event_ts, dt_fim=9999-12-31, is_current=true)
4. Persistir mudanças em Delta MERGE atômico
```

### MERGE Delta para SCD2 (forma canônica)

```sql
-- Pseudo-SQL (Delta MERGE com WHEN MATCHED ... AND _attr_hash <> ...)
MERGE INTO gold.dim_estabelecimento tgt
USING (
  SELECT
    nk_cnes,
    nome,
    tem_uti,
    leitos_existentes,
    leitos_sus,
    snapshot_date AS event_ts,
    md5(concat_ws('|', nome, tem_uti, leitos_existentes, leitos_sus)) AS _attr_hash
  FROM silver.estabelecimento_canonico
  WHERE snapshot_date = '2026-05-21'
) src
ON tgt.nk_cnes = src.nk_cnes AND tgt.is_current = true
WHEN MATCHED AND tgt._attr_hash <> src._attr_hash THEN
  UPDATE SET
    dt_fim = src.event_ts,
    is_current = false
WHEN NOT MATCHED THEN
  INSERT (sk_estabelecimento, nk_cnes, nome, tem_uti, leitos_existentes, leitos_sus,
          dt_inicio, dt_fim, is_current, _attr_hash, _loaded_at)
  VALUES (next_id_seq, src.nk_cnes, src.nome, src.tem_uti, src.leitos_existentes, src.leitos_sus,
          src.event_ts, '9999-12-31'::TIMESTAMP, true, src._attr_hash, current_timestamp());
-- Segundo MERGE para inserir a NOVA linha após o UPDATE da antiga:
-- (executado como DAG dependente — não cabe em um único MERGE Delta)
```

> **Nota:** Delta MERGE não suporta UPDATE da antiga + INSERT da nova em uma única operação atômica. Implementação real faz **dois passos** em sequência, idempotentes individualmente.

## Lookup do fato (forma canônica)

Para resolver `sk_paciente` no `fato_internacao` a partir do `nk_cpf_hash` e da `dt_inter`:

```sql
SELECT
  f.*,
  dp.sk_paciente
FROM staging_fato_internacao f
LEFT JOIN gold.dim_paciente dp
  ON dp.nk_cpf_hash = f.nk_cpf_hash
 AND f.dt_inter BETWEEN dp.dt_inicio AND dp.dt_fim
```

**Garantia:** o JOIN retorna no máximo uma linha por fato (versões SCD2 são disjuntas no tempo).

**Performance:** idealmente, `dim_paciente` tem Z-ORDER por `(nk_cpf_hash, dt_inicio)` no Delta — o Trino faz min/max pruning durante o join.

## Detecção de mudança via `_attr_hash`

Quais colunas entram no hash de cada dimensão? Decisão explícita:

### `dim_paciente`

```
_attr_hash = MD5(concat_ws('|',
  nome_mascarado,
  cep_3dig,
  data_nascimento_ano,
  sexo,
  sk_municipio_residencia
))
```

Mudanças em `nome_mascarado` ou `data_nascimento_ano` são **anomalias** (atributos imutáveis no mundo real) — disparariam alerta. Mudança em `sk_municipio_residencia` é o caso típico (paciente muda de cidade).

### `dim_estabelecimento`

```
_attr_hash = MD5(concat_ws('|',
  nome,
  tipo_unidade,
  tem_uti,
  leitos_existentes,
  leitos_sus,
  sk_municipio
))
```

Todas essas colunas podem mudar. Mudança em `sk_municipio` é raríssima (mudança de endereço de hospital).

## Inicialização (primeira carga)

Na primeira carga, todas as linhas vão para Gold como nova versão (não há versão anterior). `valid_from = data_da_primeira_carga`. Para registros pré-existentes (paciente cadastrado em 2020 mas primeira carga é 2026), há duas opções:

| Opção | Trade-off |
|---|---|
| `valid_from = data_da_primeira_carga` | Histórico antes da carga não tem dimensão para joinar (NULL) |
| `valid_from = created_at do OLTP` | Mais correto, mas exige que `created_at` exista e seja confiável |

**Decisão:** usar `created_at` quando disponível (`oltp.paciente.created_at` existe); fallback para data da carga.

## SCD2 no streaming?

A dimensão `dim_paciente` muda raramente (5% por mês — vide [`oltp-faker.md`](../../integrations/stream-faker.md)). **Atualizar SCD2 em tempo real é desperdício** — fato `fato_atendimento_stream` resolve `sk_paciente` lookup contra a versão **mais recente disponível** no Gold.

Janela: a `dim_paciente` é atualizada via DAG batch diária. Eventos de atendimento de um paciente que **acabou de mudar de endereço** podem ainda referenciar a versão antiga por algumas horas — aceitável.

**Quando esse trade-off não funcionar (em produção):** Debezium CDC do OLTP + transformação SCD2 streaming. Slide de evolução documenta.

## Consequências

### Positivas

- **Análise histórica correta:** quantas internações ocorreram quando o HC ainda não tinha UTI? Pergunta resolvida.
- **FAQ Técnico clara:** "mudou o paciente de cidade — fato antigo aponta para versão antiga; fato novo aponta para versão nova"
- **Hash de atributos** evita rewrites desnecessários — só cria nova versão quando algo realmente mudou
- **`is_current` flag** acelera queries "estado atual" sem precisar comparar `valid_to` com 9999-12-31

### Negativas (e mitigações)

- **`dim_paciente` cresce** com mudanças: 50k pacientes × 1 mudança/ano = 100k linhas em 1 ano. **Mitigação:** Delta com Z-ORDER + compaction; ainda muito menor que os fatos.
- **Lookup com BETWEEN é mais caro que igualdade simples:** mitigado por Z-ORDER por `(nk_*, valid_from)` e min/max pruning Trino/Spark
- **Lógica de carga é mais complexa:** mitigado por padronização — todas as dimensões SCD2 usam o mesmo padrão de DAG
- **Reprocessamento histórico de SCD2 é traiçoeiro:** mitigado por flag `_loaded_at` que permite identificar versões geradas em backfill diferente

## Validações automatizadas

Smoke test (vide [`smoke-test.md`](../../specs/smoke-test.md), a ser escrita) deve checar:

1. **Não há sobreposição temporal** para mesmo `nk_*`: para todo `nk`, intervalos `(dt_inicio, dt_fim)` são disjuntos
2. **Há exatamente uma versão `is_current=true`** por `nk_*` (`COUNT(*) WHERE is_current=true GROUP BY nk_*` → todo grupo = 1)
3. **`dt_fim` da versão antiga = `dt_inicio` da versão nova** quando há transição (sem gap nem overlap)
4. **`is_current=true` ⟺ `dt_fim >= '9999-01-01'`** (consistência interna)

## FAQ Técnico

| Pergunta | Resposta de 30s |
|---|---|
| Como vocês tratam SCD? | Tipo 0 em CID/Procedimento/Tempo; Tipo 1 em Município; Tipo 2 em Paciente e Estabelecimento — atributos com histórico relevante. |
| Por que SCD2 e não SCD6? | SCD2 puro com flag `is_current` cobre "atual rápido" e histórico. SCD6 adiciona complexidade sem ganho material no escopo do case. |
| Como o fato resolve a versão correta? | Lookup `BETWEEN dt_inicio AND dt_fim` na data do fato. Junções são determinísticas porque versões são disjuntas. |
| Como vocês detectam que o paciente mudou de endereço? | Hash dos atributos. Snapshot diário do OLTP gera novo hash; mudança dispara INSERT da nova versão + UPDATE da antiga. |
| E performance do lookup com BETWEEN? | Z-ORDER por `(nk, dt_inicio)` no Delta, min/max pruning Trino. Em 100k linhas é instantâneo. |
| Streaming também usa SCD2? | Não — fato streaming resolve contra versão CURRENT mais recente. Atualização SCD2 é batch diário. Trade-off de janela aceitável neste caso de uso. |
| Como vocês validam a integridade SCD2? | Smoke test com 4 asserts: sem overlap temporal, exatamente um current, transições contíguas, current ⟺ dt_fim=infinito. |

## Referências

- **Kimball — The Data Warehouse Toolkit** (capítulos 5 e 9 sobre SCD): clássico
- **Delta Lake MERGE for SCD2:** <https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables>
- **dbt snapshots (alternativa declarativa):** <https://docs.getdbt.com/docs/build/snapshots>
- **ADR 0002 idempotência** — [`./0002-idempotency-merge.md`](./0002-idempotency-merge.md)
- **Modelo dimensional** — [`../data-model.md`](../data-model.md)

---

# ADR 0007 — Spark on Kubernetes (Rancher Desktop) em vez de Spark Standalone

- **Status:** Aceito
- **Data:** 2026-07-13
- **Contexto:** decidir onde executar os jobs Spark (Bronze→Silver→Gold + Structured Streaming)

## Contexto

A solução precisa de um engine de processamento distribuído para transformações em lote e streaming. A escolha é sobre *onde* o Spark roda: em containers standalone dentro do Docker Compose, ou em Kubernetes.

## Opções avaliadas

| Critério | Spark Standalone (Compose) | **Spark on k8s (Rancher Desktop)** |
|---|---|---|
| Realismo de produção | Baixo — ninguém usa standalone em prod | **Alto — k8s é o padrão de mercado** |
| Setup local | Trivial (2 containers) | Médio (Rancher Desktop + Helm) |
| Isolamento de jobs | Compartilhado entre tasks | **Pod por job — isolamento real** |
| Escalabilidade demonstrável | Apenas vertical (mais RAM/CPU no container) | **Horizontal — novos executor pods por job** |
| Gerenciamento de recursos | Manual (`SPARK_WORKER_MEMORY`) | **ResourceQuota + LimitRange do k8s** |
| Integração com Airflow | `SparkSubmitOperator` (SSH-like) | **`SparkKubernetesOperator` (CRD declarativo)** |
| Diferencial técnico | Baixo | **Alto — demonstra domínio além do Compose** |
| Risco de demo | Baixo | Médio (mais partes, mas controlado) |

## Decisão

**Spark roda no Kubernetes provisionado pelo Rancher Desktop** (k3s local).

- `spark-on-k8s-operator` instalado via Helm no namespace `spark-operator`
- Cada job Spark é um `SparkApplication` CRD declarativo (YAML em `k8s/spark/applications/`)
- Airflow usa `SparkKubernetesOperator` montando o kubeconfig do Rancher Desktop
- Spark standalone **não entra no Docker Compose**

## Arquitetura de rede

Rancher Desktop expõe o cluster k3s na porta `6443` do host. Pods Spark acessam serviços do Compose via `host.docker.internal`:

```
Airflow (Compose) ──[kubeconfig]──→ Rancher Desktop API :6443
                                          │
                                    SparkApplication pod
                                          │
                      ┌───────────────────┼──────────────────┐
                      ↓                   ↓                   ↓
              host.docker.internal  host.docker.internal  host.docker.internal
                   :9000 (MinIO)    :5432 (Postgres)     :9092 (Kafka)
```

O `MINIO_ENDPOINT` tem valor diferente por contexto:
- Dentro do Compose (Airflow tasks Python): `http://minio:9000`
- Dentro de pods k8s (Spark jobs): `http://host.docker.internal:9000`

Essa diferença é gerenciada via variável de ambiente injetada no `SparkApplication` CRD.

## Setup reproduzível

```bash
make k8s-check        # verifica Rancher Desktop rodando + kubectl apontando
make k8s-spark-install # helm install spark-on-k8s-operator
make k8s-spark-image  # build imagem Spark custom + push para registry local
make k8s-namespace    # cria namespace spark com RBAC mínimo
```

Pré-requisito do avaliador: **Rancher Desktop instalado e rodando** (documentado no README).

## Consequências

- `docker-compose.yml` não tem `spark-master` nem `spark-worker` — mais leve
- Makefile ganha targets `k8s-*` separados dos `compose-*`
- SparkApplication YAMLs ficam em `k8s/spark/applications/` — versionados junto ao código
- Imagem Spark custom (`Dockerfile.spark`) inclui JARs: delta-core, hadoop-aws, openlineage-spark
- RAM do Rancher Desktop precisa de ≥ 8 GB alocados (configurado no Rancher Desktop Preferences)

## FAQ Técnico

*"Spark standalone existe apenas para desenvolvimento unitário sem infraestrutura. Em qualquer ambiente real — cloud ou on-premises — Spark roda no Kubernetes. Usar Rancher Desktop demonstra que a solução é cloud-ready: a mesma SparkApplication YAML que roda local sobe em EKS, GKE ou AKS sem alteração de código — apenas mudando o kubeconfig."*

---

# ADR 0008 — Separação de Camadas de Ingestão: Python (extração) + PySpark (transformação)

- **Status:** Aceito
- **Data:** 2026-07-13
- **Contexto:** definir qual ferramenta executa cada etapa do pipeline de dados

## Contexto

O pipeline de dados passa por etapas com características técnicas distintas. A decisão é sobre qual ferramenta executa cada etapa e por quê — evitando tanto subutilização (usar Spark para chamar uma API REST) quanto subutilização inversa (usar Python puro para processar Delta Lake distribuído).

## Etapas e decisões

### Etapa 1 — Extração: API REST → MinIO `landing/`

**Ferramenta escolhida: Python puro via `PythonOperator` no Airflow**

| Critério | Python requests | PySpark |
|---|---|---|
| Chamada HTTP paginada | ✅ direto (`requests.get`, loop offset) | ❌ overhead de SparkContext para I/O serial |
| Retry e rate limiting | ✅ `tenacity`, `backoff` | Complexo (não é uso nativo) |
| Modo offline (cache) | ✅ simples (`if cache_exists: load`) | Possível mas verbose |
| Tempo de startup | < 1s | 10–30s (JVM + SparkContext) |
| Output | JSON/NDJSON em `s3://landing/` | N/A |

**Justificativa:** extração é I/O serial paginado — não há dado suficiente em memória para justificar Spark. Usar Spark aqui seria overhead sem benefício e tornaria o DAG mais lento sem motivo.

### Etapa 2 — Bronze: `landing/` → `s3://bronze/` (Delta)

**Ferramenta escolhida: PySpark via `SparkKubernetesOperator`**

| Critério | Python (pandas/polars) | **PySpark** |
|---|---|---|
| Escrita Delta ACID | Via `deltalake` lib (limitada) | ✅ nativo, suporte completo a MERGE |
| Schema enforcement | Manual | ✅ `StructType` + `mergeSchema` |
| Particionamento | Manual (`os.makedirs`) | ✅ `partitionBy` no writer |
| Escalabilidade | Single-node | ✅ distribuído |
| Idempotência via MERGE | Reimplementar do zero | ✅ `DeltaTable.merge()` |

### Etapa 3 — Silver: `bronze/` → `s3://silver/` (Delta + masking PII)

**Ferramenta escolhida: PySpark**

UDFs de mascaramento (`sha2`, `substring`, `lit(null)`) são operações nativas do Spark SQL — rodam distribuídas sem código adicional. A mesma UDF funciona para batch e streaming (reutilização).

### Etapa 4 — Gold: `silver/` → `s3://gold/` + `postgres.gold_dw`

**Ferramenta escolhida: PySpark**

SCD Tipo 2 requer lógica de `MERGE` com condições complexas por `nk_*`. Delta MERGE é a implementação mais limpa e confiável. Joins dimensionais (lookup de SK) em datasets de centenas de mil linhas se beneficiam de distribuição.

### Etapa 5 — Streaming consumer: Kafka → Bronze stream → Gold stream

**Ferramenta escolhida: PySpark Structured Streaming**

Mesma API de batch — `readStream` em vez de `read`. Reutiliza UDFs de mascaramento Silver. Watermark e `foreachBatch` com MERGE Delta garantem exactly-once.

### Etapa 6 — Streaming producer: Faker → Kafka

**Ferramenta escolhida: Python puro (container `stream-producer`)**

Geração de eventos Faker e publicação em Kafka não precisa de Spark. Um loop Python com `confluent_kafka.Producer` é mais simples, mais rápido de iniciar e mais fácil de debugar.

## Fluxo completo

```
REST API (7 endpoints)
    │
    └──[PythonOperator · Airflow]──────────────────→ s3://landing/  (JSON/NDJSON)
              │
              └──[SparkKubernetesOperator · k8s]──→ s3://bronze/   (Delta)
                          │
                          └──[SparkKubernetesOperator · k8s]──→ s3://silver/  (Delta + masking)
                                        │
                                        └──[SparkKubernetesOperator · k8s]──→ s3://gold/ + postgres.gold_dw

Faker (Python container)
    │
    └──[Producer]──→ Kafka topic: notificacoes.raw
                          │
                          └──[Spark Structured Streaming · k8s]──→ s3://bronze/stream/ ──→ s3://gold/
```

## Consequências

- DAGs têm dois tipos de task: `PythonOperator` (extração) + `SparkKubernetesOperator` (transformação)
- Extratores vivem em `pipelines/extraction/` — Python puro, sem dependência Spark
- Jobs Spark vivem em `pipelines/batch/` e `pipelines/streaming/` — PySpark puro
- O pool `extraction_pool` (8 slots) limita concorrência de chamadas API
- O pool `spark_pool` (4 slots) limita jobs Spark concorrentes no k8s

## FAQ Técnico

*"Separar extração de transformação segue o princípio de responsabilidade única. A extração é I/O serial — chamar uma API, paginar, salvar em landing. Não há dado suficiente em memória para justificar a JVM do Spark. Já a transformação Bronze→Gold é computação distribuída sobre dados estruturados — exatamente o que Spark foi projetado para fazer. Misturar os dois em um único job Spark tornaria a extração mais lenta e o job mais difícil de testar."*

---

# ADR-0009 — Data Mesh: Organização por Domínios de Negócio

**Data:** 2026-07-17  
**Status:** Aceito  
**Autores:** Matheus Fideles

---

## Contexto

O projeto original organizava o código por camada técnica (`pipelines/extraction/`, `pipelines/batch/`, `pipelines/streaming/`), o que tornava difícil:

- Entender qual equipe é responsável por qual conjunto de dados
- Estabelecer SLOs por domínio de negócio
- Evoluir pipelines de um domínio sem afetar outros
- Documentar contratos de dados (schema, freshness, qualidade)

O modelo Medallion (Bronze → Silver → Gold) é sobre **como** os dados evoluem de qualidade; o Data Mesh é sobre **quem** é responsável por cada produto de dado. Os dois modelos são complementares, não excludentes.

## Decisão

Adotar **Data Mesh** como modelo organizacional ao lado da arquitetura Medallion:

1. **Domínios de negócio** como unidade primária de organização do código:
   - `apps/epidemiologico/` — dengue, zika, chikungunya, SINAN
   - `apps/hospitalar/` — CNES, SIM (óbitos)
   - `apps/geografico/` — municípios IBGE
   - `apps/vacinal/` — PNI doses aplicadas
   - `apps/streaming/` — eventos Kafka em tempo real
   - `apps/oltp/` — snapshot Postgres OLTP (PII source)

2. **Data Products** documentados com descritores YAML em `data-products/{domínio}/{produto}/dataproduct.yaml`:
   - `metadata`: nome, domínio, owner, versão, descrição, tags
   - `slo`: freshness_hours, availability_percent, quality_score_min
   - `schema`: caminhos Bronze/Silver/Gold no MinIO (Delta format)
   - `lineage`: fontes upstream e consumidores downstream
   - `ports`: interfaces de entrada (API, Kafka) e saída (Trino SQL, Delta Lake)

3. **Caminhos MinIO com prefixo de domínio:**
   - `s3a://bronze/epidemiologico/dengue/`
   - `s3a://silver/hospitalar/sim_obitos/`
   - `s3a://gold/geografico/dim_municipio/`

4. **Camada Serving via Trino + HMS** (ADR-0010): Trino expõe schemas por domínio no catálogo `delta`.

## Consequências

**Positivas:**
- Ownership claro: cada domínio tem equipe responsável e SLOs mensuráveis
- Contratos de dados explícitos (schema paths, quality thresholds)
- Isolamento: mudança no domínio `vacinal` não afeta `epidemiologico`
- Escalabilidade organizacional: cada domínio pode ter seu próprio ciclo de release
- Rastreabilidade de lineage cross-domínio via descritores YAML + Marquez

**Negativas:**
- Mais diretórios e arquivos para manter
- Curva de aprendizado para novos membros entenderem a estrutura

## Alternativas Consideradas

| Opção | Motivo da Rejeição |
|---|---|
| Organização por camada técnica | Não reflete ownership de negócio; difícil escalar times |
| Organização por fonte de dados | Acoplado à origem, não ao domínio de negócio |
| Monorepo por serviço | Overhead prematuro para o tamanho atual do projeto |

---

# ADR-0010 — Arquitetura Hexagonal (Ports & Adapters)

**Data:** 2026-07-17  
**Status:** Aceito  
**Autores:** Matheus Fideles

---

## Contexto

Jobs Spark e extratores Python acoplavam diretamente às implementações concretas (requests, boto3, psycopg2), tornando:

- Testes unitários dependentes de infra real (MinIO, Postgres, APIs externas)
- Troca de implementação (ex: S3 real vs MinIO vs cache local) exigia modificar código de negócio
- Sem separação clara entre lógica de domínio e detalhes de infraestrutura

## Decisão

Aplicar **Arquitetura Hexagonal (Ports & Adapters)** dentro de cada domínio:

```
apps/{domínio}/
├── domain/
│   ├── entities.py        # entidades de negócio (dataclasses puras)
│   └── services.py        # lógica de domínio (sem deps de infra)
├── ports/
│   ├── inbound.py         # interfaces para entrada (o que o domínio expõe)
│   └── outbound.py        # interfaces para saída (o que o domínio consome)
├── adapters/
│   ├── inbound/           # implementações concretas de entrada
│   └── outbound/          # implementações concretas de saída
└── jobs/                  # orquestradores: montam portas + adaptadores + executam
```

**Padrões adotados:**

| Padrão | Aplicação |
|---|---|
| **Template Method** | `BronzeJob`, `SilverJob`, `DimLoader`, `FatoLoader` — esqueleto fixo, passos sobrescritos |
| **Strategy** | `gold_dims.py`, `gold_fatos.py` — seleção de loader por nome em runtime |
| **DIP (Injeção de Dependência)** | `ExtractionService` recebe `DataSourcePort` e `LandingStoragePort` via construtor |
| **Adapter** | `LocalCacheAdapter`, `S3LandingAdapter`, `SingleCallAdapter`, `PaginatedAdapter` |
| **Port (Interface)** | `DataSourcePort`, `LandingStoragePort`, `CachePort` em `apps/shared/ports/` |

**Shared kernel em `apps/shared/`:**
- `spark.py` — factory de `SparkSession` com Delta + S3A configurados
- `masking.py` — funções de mascaramento PII (SHA-256+salt)
- `lineage.py` — helpers OpenLineage
- `postgres.py` — utilitários JDBC
- `bronze_base.py`, `silver_base.py`, `gold_base.py` — Template Method base classes

## Consequências

**Positivas:**
- Testes unitários isolados (mock de ports, sem infra real) → 104 testes passam sem Docker
- Troca de adaptador sem alterar lógica de domínio (ex: `OFFLINE_MODE=1` usa `LocalCacheAdapter`)
- Cada domínio tem fronteira explícita; dependências cruzadas passam por ports
- Design patterns nomeados → comunicação de equipe padronizada

**Negativas:**
- Mais boilerplate (arquivos de port/adapter mesmo quando há uma única implementação)
- Para jobs simples, a estrutura pode parecer over-engineered

## Alternativas Consideradas

| Opção | Motivo da Rejeição |
|---|---|
| Scripts planos (sem arquitetura) | Não testável; acoplamento total |
| Clean Architecture completa | Overhead maior; casos de uso bem definidos não necessários neste escopo |
| Organização só por camada (batch/streaming) | Não reflete domínios de negócio (resolvido em ADR-0009) |

---

# ADR-0011 — Serving Layer: Trino + Hive Metastore + Delta Lake

**Data:** 2026-07-17  
**Status:** Aceito  
**Autores:** Matheus Fideles

---

## Contexto

A arquitetura original conectava o Trino diretamente ao Postgres (`connector.name=postgresql`), servindo dados da camada Gold via JDBC. Isso violava o princípio do Data Lake Medalhão:

- Gold devia estar no Data Lake (MinIO/Delta), não em tabelas Postgres
- Trino conectado ao Postgres tornava o Postgres um gargalo de serving (não é seu papel)
- Sem catálogo central, Trino não conseguia descobrir tabelas Delta automaticamente
- Metabase e analistas precisariam conhecer caminhos S3 diretamente

## Decisão

Substituir o conector Postgres do Trino por **Hive Metastore (HMS) + Delta Lake connector**:

```
Metabase / Analistas
        │  SQL
        ▼
    Trino 448
    (catalog: delta)
        │  thrift://hive-metastore:9083
        ▼
  Hive Metastore 4.0.1
  (backend: PostgreSQL DB "hive")
        │  s3a://
        ▼
    MinIO (Delta Lake)
    bronze/ · silver/ · gold/
```

**Componentes adicionados:**

| Componente | Papel |
|---|---|
| `hive-metastore` (Docker service) | HMS standalone 4.0.1; armazena metadata de tabelas Delta no Postgres |
| `infra/hive-metastore/Dockerfile` | apache/hive:4.0.1 + PostgreSQL JDBC + Hadoop AWS JARs |
| `infra/hive-metastore/hive-site.xml` | Config: Postgres backend + MinIO S3A |
| `infra/postgres/init/06-hive-metastore.sql` | `CREATE USER/DATABASE hive` no Postgres |
| `infra/trino/catalog/delta.properties` | `hive.metastore=thrift`, `hive.metastore.uri=thrift://hive-metastore:9083` |

**Removidos:**
- `infra/trino/catalog/postgres.properties` — Gold não é mais servido via Postgres JDBC
- `infra/trino/catalog/minio.properties` — supersedido pelo catálogo `delta` com HMS

**Startup order:**
```
postgres (healthy) ──→ hive-metastore (healthy) ──→ trino (healthy) ──→ metabase (healthy)
minio    (healthy) ──→ hive-metastore
```

## Consequências

**Positivas:**
- Gold layer 100% no Data Lake — Postgres não é mais caminho crítico para serving
- HMS centraliza catalog: `SHOW TABLES IN delta.epidemiologico` funciona nativamente
- Trino + Delta = queries ACID sobre todo o lake (bronze, silver, gold)
- Mapeamento 1:1 para AWS: HMS → AWS Glue; MinIO → S3; Trino → Athena/Trino no EKS
- Metabase conecta ao Trino (não ao Postgres) — serving layer correta

**Negativas:**
- HMS adiciona ~90s ao tempo de startup do profile `serving`
- Imagem HMS build na primeira execução (download de JARs ~200MB)
- Tabelas Delta precisam ser registradas no HMS antes de aparecerem no Trino

## Alternativas Consideradas

| Opção | Motivo da Rejeição |
|---|---|
| Trino file-based metastore | Não suporta múltiplos schemas/domínios dinamicamente; sem catálogo central |
| Trino + Iceberg (Nessie catalog) | Iceberg exigiria reescrever todos os jobs Spark (já em Delta); migração custosa |
| Manter Postgres como serving | Gold no Postgres não é Data Lake; Postgres não escala para analytics |
| AWS Glue como metastore local | Não existe versão local do Glue; dependência de cloud |

---

