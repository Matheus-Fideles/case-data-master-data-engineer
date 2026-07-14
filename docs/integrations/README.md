# Guias de Integração — Fontes de Dados

Esta pasta contém o **plano de coleta** de cada fonte que entra na solução. Cada guia descreve **o que coletar, de onde, com qual biblioteca e quais armadilhas evitar** — sem trazer o código de extração, que fica a cargo da implementação em `pipelines/extraction/`.

## Princípios comuns a todos os guias

1. **Modo offline por default** (`OFFLINE_MODE=1` em `.env`). O candidato não pode depender de internet/APIs externas no dia da banca. Cada extrator deve respeitar esse flag e cair no cache local em `data/raw/<fonte>/...`.
2. **Cache versionado via Git LFS** quando o arquivo for grande (DataSUS DBC/CSV). JSON pequeno de API pode ir direto no Git.
3. **Determinismo:** todas as fixtures e geradores Faker usam seed fixo declarado em `.env` para que reexecuções produzam o mesmo dataset.
4. **Idempotência na escrita:** repetir extração não gera duplicação; padrão é `INSERT OVERWRITE PARTITION` no Bronze.
5. **Naming convention** dos arquivos cacheados: `data/raw/<fonte>/<dimensao>/<periodo>/<arquivo>` (ex.: `data/raw/sih/sp/2024-01/RDSP2401.csv`).

## Mapa de fontes × sprints × requisitos

| Guia | Fonte | Tipo | Sprint | Requisito do enunciado |
|---|---|---|---|---|
| [`datasus.md`](./datasus.md) | DataSUS SIH-SUS + SIM | CSV (DBC compactado, encoding latin1) | S1+S2 | Ext. (CSV) · Vol. · Variedade |
| [`cnes.md`](./cnes.md) | DataSUS CNES (Open Data API) | API REST JSON | S2 | Ext. (API) |
| [`ibge.md`](./ibge.md) | IBGE Sidra | API REST JSON | S2 | Ext. (API) · Variedade |
| [`oltp-faker.md`](./oltp-faker.md) | Postgres OLTP simulado | Banco relacional + seed Faker | S1+S2 | Ext. (BD) · Seg./Mascaramento |
| [`stream-faker.md`](./stream-faker.md) | Producer Kafka simulado | Streaming (eventos JSON) | S3 | Ing. (streaming) · Lambda |

## Como ler cada guia

Cada arquivo segue a mesma estrutura:

1. **Visão geral** — o que é a fonte e por que está no case
2. **Endpoint / FTP / formato** — onde buscar, como autenticar, rate limits
3. **Estratégia de extração** — biblioteca recomendada com alternativas
4. **Schema dos campos** — colunas relevantes (mapeamento fonte → Bronze)
5. **Particionamento e idempotência** — como persistir no lake
6. **Cache offline** — caminho, versionamento, naming
7. **Mock fallback** — fixtures para CI e dia da demo
8. **Pitfalls comuns** — armadilhas que vão custar horas se ignoradas
9. **Referências** — docs oficiais, libs, papers
