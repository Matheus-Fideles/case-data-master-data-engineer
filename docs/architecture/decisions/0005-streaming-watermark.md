# ADR 0005 — Watermark e Tratamento de Eventos Atrasados (Streaming)

- **Status:** Aceito
- **Data:** 2026-05-07
- **Decisores:** Candidato

## Contexto

A camada **speed** da arquitetura Lambda processa eventos de atendimentos publicados em Kafka pelo `stream-producer` simulado ([`stream-faker.md`](../../integrations/stream-faker.md)). Em sistemas reais, esses eventos chegam **fora de ordem** — origens espalhadas, redes intermitentes, retries do producer, replays operacionais. A banca quase certamente vai perguntar:

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

**Quando reconsiderar:** se a banca questionar especificamente, mencionar que em produção real o watermark é **negociado com o time produtor** (SLA de pontualidade).

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
- DLQ visível e analisável — defesa para "como você lida com dados ruins?"
- Restart automático graças ao checkpoint
- Watermark **demonstrável ao vivo** na banca: emitir um evento atrasado e mostrar ele caindo na DLQ
- Métricas Prometheus (`stream_dlq_count`, `stream_processing_lag`) cobrem requisito 4 (observabilidade)

### Negativas (e mitigações)

- **Estado de 1h** consome RAM proporcional ao throughput → cap de RAM no spark-worker; em produção, ajustar
- **`failOnDataLoss=false`** pode esconder problemas reais — mitigação: alerta em `kafka_data_loss_total`
- **Trigger 30s** introduz latência mínima de 30s — aceitável para o caso (saúde pública não exige tempo real <1s)
- **DLQ infinita** se ninguém olhar — política: reprocessar/expirar mensalmente

## Defesa em banca

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
