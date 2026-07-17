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
