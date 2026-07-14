# Diagrama 2 — Fluxo de Dados (Bronze → Silver → Gold)

**Audiência:** banca técnica
**Pergunta que responde:** *"Como o dado se transforma da chegada até estar pronto para análise? Onde é mascarado? Como é versionado?"*

## Fluxo completo

```mermaid
flowchart TB
    subgraph EXT["EXTRAÇÃO"]
        direction LR
        E1["CSV DataSUS<br/>📦 lote mensal"]
        E2["API CNES/IBGE<br/>📦 chamada paginada"]
        E3["Postgres OLTP<br/>📦 snapshot diário"]
        E4["Stream Faker<br/>⚡ eventos contínuos"]
    end

    subgraph BRONZE["🥉 BRONZE — RAW (com PII)"]
        direction TB
        BR1["delta://bronze/sih_sus<br/>partição: ano_mes"]
        BR2["delta://bronze/cnes<br/>partição: ano_mes"]
        BR3["delta://bronze/pacientes<br/>partição: data_snapshot"]
        BR4["delta://bronze/atendimentos_stream<br/>partição: ano_mes_dia"]
        BR_NOTE["⚠️ ÚNICO local com PII real<br/>Acesso restrito<br/>Retenção: 7 dias após mascaramento<br/>Schema evolution: mergeSchema=true"]
    end

    subgraph SILVER["🥈 SILVER — LIMPO + MASCARADO"]
        direction TB
        SI1["delta://silver/sih_sus<br/>tipos validados"]
        SI2["delta://silver/cnes"]
        SI3["delta://silver/pacientes<br/>🔒 mascarado"]
        SI4["delta://silver/atendimentos<br/>🔒 mascarado"]
        SI_MASK["MASCARAMENTO APLICADO AQUI<br/>━━━━━━━━━━━━━━━━━━━━━━<br/>CPF      → SHA-256 + salt secreto<br/>Nome     → Faker determinístico<br/>CEP      → 3 primeiros dígitos<br/>Data nasc → ano apenas<br/>Endereço → suprimido"]
    end

    subgraph GOLD["🥇 GOLD — STAR SCHEMA"]
        direction LR
        subgraph DIMS["Dimensões"]
            G_DP["dim_paciente<br/>SCD2"]
            G_DE["dim_estab<br/>SCD2"]
            G_DM["dim_municipio<br/>SCD1"]
            G_DC["dim_cid"]
            G_DT["dim_tempo"]
        end
        subgraph FATOS["Fatos"]
            G_FI["fato_internacao<br/>MERGE por nk_aih"]
            G_FO["fato_obito<br/>MERGE por nk_dec"]
            G_FA["fato_atendimento_stream<br/>MERGE por sk_paciente+ts"]
        end
    end

    subgraph DW["DATA WAREHOUSE"]
        DWP[("Postgres Gold<br/>réplica via dbt/Spark")]
    end

    subgraph CONSUMO["CONSUMO"]
        CON1["Trino"]
        CON2["Metabase"]
    end

    E1 -->|Airflow DAG| BR1
    E2 -->|Airflow DAG| BR2
    E3 -->|Airflow DAG| BR3
    E4 -->|Spark Streaming| BR4

    BR1 -->|PySpark batch| SI1
    BR2 -->|PySpark batch| SI2
    BR3 -->|PySpark batch + UDFs mask| SI3
    BR4 -->|Spark Streaming + UDFs mask| SI4

    SI1 --> G_FI
    SI1 --> G_FO
    SI2 --> G_DE
    SI3 --> G_DP
    SI4 --> G_FA

    G_DP -.lookup SCD2.-> G_FI
    G_DP -.lookup SCD2.-> G_FO
    G_DP -.lookup SCD2.-> G_FA
    G_DE -.lookup SCD2.-> G_FI
    G_DE -.lookup SCD2.-> G_FA
    G_DM -.lookup.-> G_FI
    G_DC -.lookup.-> G_FI
    G_DC -.lookup.-> G_FO
    G_DT -.lookup.-> G_FI
    G_DT -.lookup.-> G_FO
    G_DT -.lookup.-> G_FA

    GOLD --> DWP
    GOLD --> CON1
    DWP --> CON1
    CON1 --> CON2

    style EXT fill:#e3f2fd
    style BRONZE fill:#efebe9
    style SILVER fill:#eceff1
    style GOLD fill:#fff8e1
    style SI_MASK fill:#ffebee,stroke:#c62828,stroke-width:2px
    style BR_NOTE fill:#fff3e0,stroke:#ef6c00
```

## Decisões de fluxo destacadas

### 🔒 Mascaramento — onde e como

| Coluna sensível | Técnica | Reversível? | Justificativa LGPD |
|---|---|---|---|
| CPF | SHA-256 + salt secreto (Vault em prod, env em dev) | Não | Anonimização irreversível; preserva join determinístico |
| Nome | Faker com seed = hash(nk_cpf) | Não | Aparência realista para análise; não rastreia identidade |
| Data de nascimento | Generalização: só o ano | Parcial | Permite faixa etária; remove identificação fina |
| CEP | 3 primeiros dígitos (microrregião) | Parcial | Permite análise geográfica regional |
| Endereço completo | Supressão | — | Não há valor analítico justificável |
| Telefone, e-mail | Supressão | — | Idem |

**Aplicação:** UDFs PySpark em `pipelines/common/masking.py`. Mesma função usada por batch e streaming (princípio de não duplicar lógica).

### ⚙️ Idempotência por camada

| Transição | Estratégia | Razão |
|---|---|---|
| Fonte → Bronze | `INSERT` append + dedupe por hash de linha | Bronze preserva tudo; dedupe protege contra reentrega Kafka |
| Bronze → Silver | `INSERT OVERWRITE PARTITION (ano_mes)` | Reprocessar mês inteiro é seguro e barato |
| Silver → Gold (dim) | `MERGE` com lógica SCD2 | Atualiza `valid_to` da versão antiga, insere nova |
| Silver → Gold (fato) | `MERGE` por natural key | Reprocessamento de mesma AIH não duplica linha |
| Stream → Bronze | `foreachBatch` + `MERGE` por `(paciente, ts_evento)` | Garantia exactly-once com Delta |

### 🕒 Watermark e janela de streaming

- **Watermark:** 1 hora (eventos atrasados além disso são descartados e logados em DLQ).
- **Janela de agregação:** tumbling de 5 minutos para métrica `atendimentos_por_estabelecimento`.
- **Trigger:** processing time = 30s (responsivo na demo, sem CPU constante).

### 🔁 Schema evolution

- **Bronze:** `mergeSchema=true` no `writeStream`/`write` → aceita colunas novas sem quebrar.
- **Silver:** schema explícito declarado em `pipelines/common/schemas.py` → quebrar é intencional.
- **Gold:** schema versionado em dbt/SQL → migrations explícitas via dbt seeds ou Alembic.

### 🧪 Validação de qualidade (Great Expectations)

Pontos de validação obrigatórios nos DAGs:
- **Pós-Bronze:** schema mínimo, contagem > 0, nulos abaixo de threshold por coluna crítica
- **Pós-Silver:** unicidade de natural keys, integridade referencial
- **Pós-Gold:** integridade do star schema (todo fato resolve para dimensão atual ou versionada)

Falha em qualquer expectation → DAG marca falha, alerta no Grafana, dado fica retido no estágio anterior.
