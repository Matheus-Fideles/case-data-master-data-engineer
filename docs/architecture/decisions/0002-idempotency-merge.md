# ADR 0002 — Estratégia de Idempotência e MERGE

- **Status:** Aceito
- **Data:** 2026-05-07
- **Decisores:** Candidato

## Contexto

Pipelines de dados falham — rede cai, cluster reinicia, container morre. **Reexecutar a mesma carga sem corromper o resultado** é requisito mínimo de qualquer plataforma séria. A banca certamente vai perguntar:

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

**Cuidado:** se uma fonte mudou entre a primeira carga e o backfill (ex.: DataSUS publicou correção do arquivo), o backfill propaga a versão atual. Documentar é a única defesa: log do `_source_file` checksum em metadata permite identificar.

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

- **Reexecução previsível** — banca pode pedir "rode duas vezes" e ver o mesmo resultado
- **Backfill seguro** — janela histórica reprocessável sem caos
- **Defesa direta** para perguntas "e se Kafka reentregar?" e "e se DAG falhar no meio?"
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

## Apêndice — defesa em banca

| Pergunta | Resposta de 30s |
|---|---|
| Se o DAG rodar 2x, duplica? | Não. Bronze faz `INSERT OVERWRITE PARTITION`; mesma execução produz mesmo resultado. |
| Como você garante exactly-once com Kafka? | Producer idempotente + `foreachBatch` MERGE no Spark Streaming, com chave `(id_paciente, ts_evento)`. |
| Backfill 5 anos é seguro? | Sim — cada partição mensal é independente; rodam em paralelo via Airflow `max_active_runs` controlado. |
| O MERGE no Silver é caro? | Mitigado por `_row_hash` que detecta mudança antes de rewrite — quase tudo vira NO-OP em rerun limpo. |
| E se o Kafka cair durante a demo? | Producer faz buffer e retry; consumer reinicia do checkpoint S3, sem perda nem duplicação. |
