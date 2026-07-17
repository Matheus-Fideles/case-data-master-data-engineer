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
