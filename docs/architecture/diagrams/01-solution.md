# Diagrama 1 — Arquitetura de Solução

**Audiência:** banca avaliadora + leitor de negócio
**Pergunta que responde:** *"O que essa solução faz, de onde vem o dado e quem consome?"*

```mermaid
flowchart LR
    subgraph FONTES["📥 FONTES DE DADOS"]
        direction TB
        F1[("CSV<br/>DataSUS<br/>SIH-SUS · SIM")]
        F2{{"API REST<br/>CNES<br/>IBGE Sidra"}}
        F3[(Postgres<br/>OLTP<br/>cadastro pacientes)]
        F4(("Stream<br/>Faker<br/>atendimentos RT"))
    end

    subgraph PLATAFORMA["🏗️ PLATAFORMA DE DADOS — Arquitetura Lambda"]
        direction TB
        subgraph BATCH["Camada Batch"]
            B1["Airflow<br/>orquestração"]
            B2["PySpark<br/>processamento"]
        end
        subgraph SPEED["Camada Speed (tempo real)"]
            S1["Kafka<br/>backbone"]
            S2["Spark Structured<br/>Streaming"]
        end
        subgraph LAKE["Data Lake — MinIO + Delta Lake"]
            L1["🥉 Bronze<br/>raw"]
            L2["🥈 Silver<br/>limpo + mascarado"]
            L3["🥇 Gold<br/>star schema"]
        end
        subgraph DW["Data Warehouse"]
            D1[("Postgres<br/>Gold dimensional")]
        end
    end

    subgraph CONSUMO["📊 CONSUMO"]
        direction TB
        C1["Trino<br/>query federada"]
        C2["Metabase<br/>dashboards"]
        C3["Notebooks<br/>análise ad-hoc"]
    end

    subgraph TRANSV["🛡️ CAMADA TRANSVERSAL"]
        direction LR
        T1["Segurança<br/>TLS · AES-256 · RBAC · LGPD"]
        T2["Observabilidade<br/>Prometheus · Grafana · Marquez"]
        T3["Qualidade<br/>Great Expectations"]
    end

    F1 --> B1
    F2 --> B1
    F3 --> B1
    F4 --> S1
    
    B1 --> B2
    B2 --> L1
    S1 --> S2
    S2 --> L1
    
    L1 --> L2
    L2 --> L3
    L3 --> D1
    
    L3 --> C1
    D1 --> C1
    C1 --> C2
    C1 --> C3

    PLATAFORMA -.observa.-> TRANSV

    style FONTES fill:#e3f2fd,stroke:#1976d2
    style PLATAFORMA fill:#f3e5f5,stroke:#7b1fa2
    style CONSUMO fill:#e8f5e9,stroke:#388e3c
    style TRANSV fill:#fff3e0,stroke:#f57c00
    style BATCH fill:#fce4ec
    style SPEED fill:#e1f5fe
    style LAKE fill:#fff9c4
    style DW fill:#f1f8e9
```

## Leitura guiada

1. **À esquerda** — quatro fontes representando os formatos pedidos pelo enunciado (CSV, API, BD, stream).
2. **Centro (azul/roxo)** — a plataforma com duas camadas paralelas (Lambda): batch para o que chega periodicamente (DataSUS), speed para o que chega em fluxo contínuo (atendimentos simulados).
3. **Lake medalhão** — Bronze (raw, com PII), Silver (limpo + **mascarado**), Gold (modelo dimensional pronto para análise).
4. **Postgres DW** — espelha o Gold para queries OLAP rápidas e RBAC granular.
5. **À direita** — consumo via Trino (query federada) e Metabase (visualização final).
6. **Embaixo (laranja)** — camada transversal cobrindo segurança, observabilidade e qualidade.

## Mapeamento requisito → componente neste diagrama

| Requisito do enunciado | Onde aparece |
|---|---|
| 1. Extração | Bloco "Fontes de Dados" — 4 formatos distintos |
| 2. Ingestão | Camadas Batch e Speed (Lambda) |
| 3. Armazenamento | Lake (MinIO+Delta) + DW (Postgres) |
| 4. Observabilidade | Camada Transversal — Prometheus/Grafana/Marquez |
| 5. Segurança | Camada Transversal — TLS/AES/RBAC/LGPD |
| 6. Mascaramento | Bronze→Silver explicitado como "limpo + mascarado" |
| 7. Arquitetura | Medalhão + lake + DW + processamento distribuído (Spark) |
| 8. Escalabilidade | Spark distribuído + Kafka particionado (slide cloud detalha 10x) |
