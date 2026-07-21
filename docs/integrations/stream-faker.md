# Integração: Stream Producer — Atendimentos em Tempo Real

> **Sprint:** S3
> **Requisitos cobertos:** Ingestão streaming · Arquitetura Lambda · Tempo real · Variedade

## 1. Visão geral

Esta "fonte" é um **producer Python rodando como container** que publica **eventos sintéticos de atendimentos** (consultas, atendimentos de pronto-socorro, internações sendo abertas) em um tópico Kafka, simulando um sistema hospitalar emitindo eventos em tempo real.

É o **caminho speed da arquitetura Lambda** — sem ele, a arquitetura fica exclusivamente batch.

**Por que stream simulado em vez de fonte real:**
- Não existe stream público real e estável de saúde para se conectar (CNS/RNDS exige autenticação institucional)
- Volumetria controlada (5 eventos/seg ou 5000 — você decide na hora da demo)
- Pareamento garantido com o OLTP (FK para pacientes existentes que serão mascarados)
- Reprodutibilidade total

## 2. Topologia

```
[stream-producer container]
        │
        │ confluent-kafka producer
        ▼
   Kafka topic: atendimentos.raw          (eventos JSON, válidos)
   Kafka topic: atendimentos.dlq          (eventos malformados — em S3 quando demonstrarmos resiliência)
        │
        │ Spark Structured Streaming consumer
        ▼
   Bronze:    s3a://bronze/atendimentos_stream/...
        │
        ▼
   Silver:    s3a://silver/atendimentos/...    (após mascaramento)
        │
        ▼
   Gold:      fato_atendimento_stream         (MERGE com janela de 1h)
```

## 3. Schema do evento

### Campos obrigatórios

| Campo | Tipo | Origem | Comentário |
|---|---|---|---|
| `id_atendimento` | UUID v4 | Faker | Único por evento |
| `id_paciente` | int (FK paciente.id) | Sample do Postgres OLTP | **Importante:** existir na fonte para join determinístico |
| `cnes_estabelecimento` | string(7) | Sample da OLTP | FK estabelecimento |
| `ts_evento` | ISO8601 com TZ | Faker (próximo de now) | **Event time** — usado pelo watermark |
| `tipo_atendimento` | enum | Random | `pronto_socorro`, `consulta`, `internacao_aberta`, `urgencia` |
| `sintomas_cid` | array de string(4) | Sample CID-10 | 1-3 códigos por evento (multi-morbidade) |
| `triagem` | enum | Random com peso | `verde`, `amarela`, `vermelha` (Manchester) |
| `producer_version` | string | const | `"1.0.0"` (rastreabilidade) |
| `schema_version` | int | const | `1` (incrementa em mudança breaking) |

### Exemplo de payload

```json
{
  "id_atendimento": "8f7a3b2e-1c4d-4e5f-9a0b-2c3d4e5f6a7b",
  "id_paciente": 4271,
  "cnes_estabelecimento": "2077426",
  "ts_evento": "2026-05-21T14:32:18.245-03:00",
  "tipo_atendimento": "pronto_socorro",
  "sintomas_cid": ["I10", "E11"],
  "triagem": "amarela",
  "producer_version": "1.0.0",
  "schema_version": 1
}
```

### Decisão: JSON vs Avro

| Critério | JSON | Avro + Schema Registry |
|---|---|---|
| Simplicidade | ✅ alta | média |
| Schema validation | manual ou Pydantic | nativa via registry |
| Tamanho do payload | 2–3x maior | compacto |
| FAQ Técnico | "fui pragmático" | "padrão de mercado" |
| Esforço de setup | baixo | adiciona Schema Registry no Compose |

**Recomendação:** começar com **JSON + Pydantic** para validação client-side; Avro documentado como evolução futura.

## 4. Estratégia de produção

### Biblioteca: **confluent-kafka-python**

```bash
pip install confluent-kafka pydantic faker
```

Vantagens sobre `kafka-python`:
- Wrapper de librdkafka (C) — performance superior
- Suporta exactly-once semantics se um dia precisar
- Mantida pelo time da Confluent

Alternativa: `kafka-python` (puro Python) — mais simples mas mais lento.

### Padrão de operação (semântica, não código)

O container `stream-producer` deve:

1. Esperar Kafka ficar disponível (loop com timeout 60s no startup)
2. Carregar lista de pacientes existentes do Postgres OLTP no boot (~50k IDs)
3. Carregar lista de estabelecimentos (~500 CNES)
4. Loop principal:
   - Gerar evento sintético com Faker
   - Validar com Pydantic (proteção contra schema drift no próprio gerador)
   - Publicar em `atendimentos.raw` com chave = `id_paciente` (preserva ordem por paciente)
   - Sleep configurável (default 200ms = 5 eventos/seg)
5. Em modo **demo**, aceitar `EVENTS_PER_SECOND` via env (1 a 100)
6. Em modo **stress**, aceitar `BURST_MODE=1` para gerar 1000 eventos rápido depois pausar (testa watermark)
7. Suportar **graceful shutdown** (SIGTERM): flush do producer + log de total emitido

### Configuração do producer Kafka

| Configuração | Valor sugerido | Razão |
|---|---|---|
| `bootstrap.servers` | `kafka:9092` | Endereço interno da rede Compose |
| `client.id` | `stream-producer-1` | Auditoria |
| `acks` | `all` | Garantia de durabilidade |
| `enable.idempotence` | `true` | Evita duplicação por retry |
| `compression.type` | `gzip` | Reduz tráfego no laptop |
| `linger.ms` | `100` | Batch para eficiência |
| `batch.size` | `16384` | Default OK |

### Particionamento

- Tópico `atendimentos.raw` com **3 partições** (suporta escalabilidade horizontal)
- **Chave = `id_paciente`** garante que eventos do mesmo paciente caem na mesma partição (ordem preservada para Spark)
- `id_paciente` UUID seria pior (espalha tudo); usar o int da OLTP

## 5. Sub-fluxo: dirty events para DLQ (S3 demonstrativo)

Para demonstrar resiliência:
- **5% dos eventos**: producer emite payload **propositalmente quebrado** (campo faltando, tipo errado, CID inválido)
- Configurável via `DIRTY_RATE=0.05`
- Spark Streaming consumer detecta e desvia para `atendimentos.dlq`
- Dashboard Grafana mostra contagem DLQ → demonstração de monitoramento de gargalos (req. 4)

## 6. Como o Spark consome

Detalhado em [`ADR 0005 streaming watermark`](../architecture/decisions/0005-streaming-watermark.md) (a ser escrito). Resumo:

- Trigger: processing time, 30 segundos (responsivo para demo, sem CPU constante)
- Watermark: 1 hora — eventos com `ts_evento` mais antigo que `max(ts_evento) - 1h` são descartados (logados em DLQ)
- Idempotência: `MERGE` Delta no `foreachBatch` por chave `(id_paciente, ts_evento)`
- Output: append em `s3a://bronze/atendimentos_stream/ano_mes_dia=YYYY-MM-DD/`

## 7. Cache offline / fallback

Stream não é "cacheável" no mesmo sentido que CSV/API — é tempo real por natureza. Mas:

- **Em CI** (smoke test): rodar producer por **5 segundos**, capturar saída, validar consumer recebeu
- **Em demo offline** (laptop sem rede): producer gera tudo localmente — não depende de internet
- **Replay de eventos antigos** (reprodução ao estilo Kappa): producer pode ler `data/raw/atendimentos/replay_<dia>.jsonl` em vez de gerar novos — é como fazer "playback" de uma demo passada. Útil para gravação de vídeo de fallback.

## 8. Volume e dimensionamento

| Modo | Eventos/seg | Eventos em 5 min | Uso de RAM laptop |
|---|---|---|---|
| Demo lenta | 1 | 300 | trivial |
| Demo padrão | 5 | 1500 | trivial |
| Demo "wow" | 50 | 15000 | OK em laptop com 16 GB |
| Stress test | 500 | 150000 | Spark começa a usar mais RAM |
| Backfill replay | 10000 | 3M | só com Spark cluster real |

**Recomendação para a demo de 1h30:** começar em 5/seg para a parte técnica, subir para 50/seg na seção de "escalabilidade" para mostrar Grafana com gráfico animado.

## 9. Pitfalls comuns

| Pitfall | Sintoma | Mitigação |
|---|---|---|
| `id_paciente` aleatório (UUID) que não existe na OLTP | Join no Silver vira NULL | **Sempre samplar IDs reais** carregados no boot |
| `ts_evento` com timezone errado | Janela de 1h dispara errado | **UTC sempre**, converter na visualização |
| Producer não fecha graceful → mensagens perdidas | Contagem diverge | SIGTERM handler com `producer.flush(30)` |
| `bootstrap.servers=kafka:9092` quando producer roda fora do Compose | Connection refused | Usar `localhost:29092` (advertised listener externo) ou rodar dentro da rede |
| Faker `.cid()` não existe | AttributeError | Sample manual de uma lista de CIDs frequentes (top 50 do SIH) |
| Burst de 1k eventos em laptop fraco | Spark trava | Configurar `maxOffsetsPerTrigger=1000` no consumer |
| Tópico não criado antes do producer iniciar | `UNKNOWN_TOPIC_OR_PARTITION` | KAFKA_AUTO_CREATE_TOPICS_ENABLE=true (S1 default) ou criar explicitamente no startup |
| Schema mudou e producer/consumer divergem | Bronze cresce mas Silver não processa | Versionamento de schema (`schema_version` no payload) + log de versões observadas |

## 10. FAQ Técnico (perguntas previsíveis)

| Pergunta | Resposta curta |
|---|---|
| Por que stream simulado e não Kafka Connect com fonte real? | Fonte real de saúde exige integração institucional (RNDS); o caso pede demonstração técnica, não dado real |
| Como garantir que o `id_paciente` do stream existe no OLTP? | Producer carrega lista de IDs no boot e samplea — invariante de FK preservada |
| E se o Kafka cair durante a demo? | Producer faz buffer interno + retry idempotente; consumer Spark relê do offset checkpoint |
| Por que JSON e não Avro? | Pragmatismo no escopo; Avro+Schema Registry está no roadmap (slide de evolução) |
| Como você escala isso para 100k eventos/seg? | Mais partições no tópico + replicação=3 + producers em paralelo + Spark com mais executors. Slide cloud (MSK + EMR) detalha. |

## 11. Referências

- **confluent-kafka-python:** <https://github.com/confluentinc/confluent-kafka-python>
- **librdkafka configs:** <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>
- **Pydantic v2:** <https://docs.pydantic.dev/latest/>
- **Spark Structured Streaming + Kafka:** <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>
- **Triagem Manchester (sintomas/triagem):** referência clínica BR
- **CID-10 lista frequente:** <https://datasus.saude.gov.br/wp-content/uploads/2022/01/CID-10.pdf>
