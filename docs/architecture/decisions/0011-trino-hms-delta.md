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
