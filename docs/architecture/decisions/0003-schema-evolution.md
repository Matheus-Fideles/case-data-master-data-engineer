# ADR 0003 — Política de Schema Evolution

- **Status:** Aceito
- **Data:** 2026-05-07
- **Decisores:** Candidato

## Contexto

Fontes de dados externas mudam:

- **DataSUS** já mudou o layout do SIH-SUS pelo menos 3 vezes em 20 anos (acréscimo de colunas como `ETNIA`, `RACA_COR`)
- **CNES API** adicionou `tem_uti` em 2022 — campo novo entrou silenciosamente na resposta JSON
- **IBGE Sidra** mantém compatibilidade, mas ocasionalmente publica novo agregado que substitui antigo

Sem política, o pipeline quebra a cada mudança upstream e o backfill histórico fica refém. A banca certamente vai perguntar:

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

## Defesa em banca

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
