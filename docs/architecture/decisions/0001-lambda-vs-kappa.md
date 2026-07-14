# ADR 0001 — Arquitetura Lambda vs Kappa

- **Status:** Aceito
- **Data:** 2026-05-07
- **Decisores:** Candidato (responsável pelo case)
- **Contexto do case:** Academia Santander — Engenharia de Dados

## Contexto

O case exige que a solução seja capaz de ingerir dados de múltiplas fontes (CSV, APIs públicas, banco de dados relacional) com **diferentes características de chegada**:

| Fonte | Cadência natural | Volume típico |
|---|---|---|
| SIH-SUS (internações DataSUS) | Lote mensal (arquivos consolidados) | Centenas de MB por UF/mês |
| SIM (mortalidade DataSUS) | Lote anual/mensal | Centenas de MB |
| CNES (estabelecimentos) | API REST, atualização mensal | KB-MB |
| IBGE (população municipal) | API REST, atualização anual | KB |
| Postgres OLTP (cadastro de pacientes simulado) | Mudanças contínuas (CDC) | Linhas por minuto |
| Stream simulado de atendimentos (Faker) | Eventos contínuos (tempo real) | Eventos por segundo |

A banca menciona explicitamente "arquiteturas Kappa, Lambda" no requisito 2 (Ingestão), portanto a decisão precisa ser **declarada e justificada**.

## Opções avaliadas

### Opção A — Arquitetura Lambda (escolhida)

Dois caminhos paralelos com convergência no Gold:

- **Camada batch:** Airflow orquestra extração mensal/diária de CSV DataSUS, APIs CNES/IBGE e snapshots do Postgres OLTP → MinIO Bronze → PySpark Silver → modelagem dimensional Gold.
- **Camada speed (streaming):** Producer simulado publica atendimentos em Kafka → Spark Structured Streaming consome → grava direto na Bronze (Delta Lake append) → janela de agregação alimenta tabela Gold de "atendimentos em tempo real".
- **Convergência:** Trino consulta Gold (batch) + Gold (speed) na mesma camada lógica; Metabase exibe ambos em dashboards.

**Vantagens neste case:**
- Honra a natureza real das fontes: DataSUS publica em lote consolidado mensal; forçar streaming nesse caminho é artificial e indefensável na banca.
- Permite mostrar **dois pipelines distintos** funcionando ao vivo na demo de 1h30, evidenciando domínio dos dois paradigmas.
- Falha de um caminho não derruba o outro (resiliência).

**Desvantagens:**
- Duplicação de lógica de negócio entre batch e speed (atenuada com biblioteca compartilhada `pipelines/common/`).
- Mais código para manter (mitigado pelo escopo limitado do case).

### Opção B — Arquitetura Kappa (rejeitada)

Único caminho via streaming. CSVs do DataSUS seriam "rebobinados" (replay) através de Kafka como se fossem eventos.

**Por que foi rejeitada:**
- Modelo conceitualmente forçado: arquivos do DataSUS são publicações batch consolidadas mensais, não eventos contínuos. Ingerir 30 dias num único Kafka topic e fingir que são eventos compromete corretude técnica e é difícil de defender na banca.
- Infraestrutura mais cara: backfill histórico via stream exige Kafka com retenção longa (TB) — inviável em laptop.
- Esconde a competência batch (orquestração, dependências entre tarefas, idempotência) que a banca espera ver.

### Opção C — Apenas batch (rejeitada)

Ignorar streaming completamente.

**Por que foi rejeitada:**
- Requisito 2 cita "tempo real" explicitamente.
- Requisito 8 cita "demanda crescente por análises em tempo real".
- A banca **vai** perguntar como a solução lida com streaming.

## Decisão

**Adotar Arquitetura Lambda** com:

1. **Batch layer:** Airflow + PySpark + MinIO/Delta + Postgres DW.
2. **Speed layer:** Kafka + Spark Structured Streaming + Delta append + agregação por janela.
3. **Serving layer:** Trino federando Gold batch e Gold speed; Metabase como ponto único de visualização.

## Consequências

### Positivas
- Resposta direta e correta às perguntas da banca sobre Kappa vs Lambda.
- Demonstração visual de dois pipelines distintos vivos na apresentação.
- Resiliência: falha em um caminho não interrompe o outro.
- Fontes batch e stream tratadas conforme sua natureza real.

### Negativas (e mitigações)
- **Duplicação de lógica:** mitigada por `pipelines/common/` (mascaramento, validações, lineage compartilhados entre batch e stream).
- **Custo cognitivo de duas pipelines:** mitigado pelo escopo enxuto (1 fato batch principal + 1 fato streaming).
- **Reconciliação batch×stream:** as duas camadas escrevem em tabelas Gold separadas (`gold.fato_internacao` e `gold.fato_atendimento_stream`) — não há merge complexo neste case.

## Evolução futura (slide cloud)

Em produção AWS:
- Batch layer: EMR + S3 + Glue Catalog + Redshift.
- Speed layer: MSK + Kinesis Data Analytics ou EMR Streaming.
- Serving: Athena (federado) + QuickSight.

## Referências
- Marz, N. & Warren, J. *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*.
- Kreps, J. *"Questioning the Lambda Architecture"* (defesa de Kappa) — referenciada para mostrar conhecimento dos dois lados.
