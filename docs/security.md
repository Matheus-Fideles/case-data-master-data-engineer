# Política de Segurança e LGPD

> **Audiência:** banca avaliadora · revisores de segurança · candidato
> **Status:** Política normativa do case. Cobre os requisitos 5 (Segurança) e 6 (Mascaramento) do enunciado, articulando criptografia, controle de acesso, mascaramento e LGPD.
> **Última revisão:** 2026-05-07

## Índice

1. [Objetivo e alcance](#1-objetivo-e-alcance)
2. [Princípios](#2-princípios)
3. [Classificação dos dados](#3-classificação-dos-dados)
4. [LGPD — base legal e finalidades](#4-lgpd--base-legal-e-finalidades)
5. [Criptografia](#5-criptografia)
6. [Controle de acesso (RBAC)](#6-controle-de-acesso-rbac)
7. [Mascaramento e anonimização](#7-mascaramento-e-anonimização)
8. [Retenção e direito ao esquecimento](#8-retenção-e-direito-ao-esquecimento)
9. [Segregação por camada do lake](#9-segregação-por-camada-do-lake)
10. [Gestão de segredos](#10-gestão-de-segredos)
11. [Auditoria e rastreabilidade](#11-auditoria-e-rastreabilidade)
12. [Modelo de ameaças](#12-modelo-de-ameaças)
13. [Backup e recuperação](#13-backup-e-recuperação)
14. [Evolução cloud (slide)](#14-evolução-cloud-slide)
15. [Defesa em banca — perguntas e respostas](#15-defesa-em-banca--perguntas-e-respostas)

---

## 1. Objetivo e alcance

Este documento estabelece as práticas de **segurança e privacidade** aplicadas à solução de Engenharia de Dados do case Santander. Cobre todos os componentes em escopo do entregável: extratores, pipelines, lake (Bronze/Silver/Gold), DW, query federada e visualização.

**Fora de escopo:** segurança de rede física, segurança da infraestrutura cloud (delegada a S3 KMS, IAM, etc. — descrito como evolução), pentest e segurança operacional do laptop.

## 2. Princípios

1. **Privacy by design** — segurança e privacidade são requisitos funcionais, não adendos
2. **Least privilege** — cada componente, role e usuário acessa apenas o estritamente necessário
3. **Defense in depth** — múltiplas camadas: criptografia + RBAC + mascaramento + auditoria
4. **PII só persiste no Bronze** — qualquer linha que sai do Bronze para o Silver é mascarada
5. **Segredos não estão no código** — sempre em variável de ambiente (dev) ou cofre (prod)
6. **Auditoria por padrão** — toda movimentação de dado emite evento OpenLineage e log estruturado
7. **Anonimização preferível a pseudonimização** — quando viável (preservar joins é viável; reverter identidade não é)

## 3. Classificação dos dados

| Classe | Definição | Exemplos no case | Onde permanece |
|---|---|---|---|
| **PII direto** | Identificador único e sensível | CPF, nome, e-mail, telefone | Apenas Bronze (`bronze.oltp_paciente`); some no Silver |
| **PII generalizável** | Identificador parcial (quasi-identifier) | Data nascimento, CEP completo, endereço | Bronze; generalizado no Silver |
| **Dados de saúde** | Diagnósticos, procedimentos, óbito | CID-10, AIH, DO | Bronze, Silver, Gold (sem PII direto) |
| **Dados públicos** | Dados abertos do MS, IBGE | População, lista CNES, CID-10 | Todas as camadas |
| **Metadados operacionais** | `_ingested_at`, `_source_run_id` | — | Todas as camadas |

> **Nota:** dados de saúde **per se** não são PII direto, mas combinados com PII direto se tornam dados sensíveis pela LGPD (Art. 5º, II). Nossa estratégia é **garantir que o vínculo direto seja quebrado** antes do Silver.

## 4. LGPD — base legal e finalidades

### 4.1 Aplicabilidade

A LGPD (Lei 13.709/2018) aplica-se ao tratamento de dados pessoais de pessoas naturais. **Os dados deste case são sintéticos** (gerados via Faker), portanto a LGPD literalmente não se aplica. Mas a solução é **desenhada como se fosse produção**, demonstrando que o framework atende à LGPD se um dia for plugado em fonte real.

### 4.2 Bases legais aplicáveis (em produção real)

Para o caso hipotético de uso real:

| Tratamento | Base legal LGPD aplicável | Artigo |
|---|---|---|
| Carga de dados de saúde do SUS | Tutela da saúde (saúde pública) | Art. 11, II, "f" |
| Análise estatística agregada | Estudos por órgão de pesquisa, com anonimização | Art. 11, II, "c" |
| Visualização em dashboard interno (analistas) | Legítimo interesse + obrigação regulatória | Art. 7º, II e IX |

### 4.3 Finalidade explícita

O tratamento se restringe a:
- Análise epidemiológica agregada (taxas, distribuições, séries temporais)
- Apoio à gestão de capacidade hospitalar (leitos, UTI, permanência)

Qualquer uso fora dessas finalidades exige nova base legal e novo aceite do DPO.

### 4.4 Direitos do titular

Quando aplicável (dados reais), o titular pode:
- **Confirmar** existência de tratamento (`SELECT count(*) FROM bronze.oltp_paciente WHERE cpf_hash = ?`)
- **Acessar** dados — com cuidado para não vazar a chave hash em log
- **Corrigir** — UPDATE no OLTP + reprocessamento Silver/Gold
- **Eliminar** ("direito ao esquecimento") — vide §8

## 5. Criptografia

### 5.1 Em trânsito (in-transit)

| Componente | Estado atual (S1) | Alvo S3 | Alvo produção |
|---|---|---|---|
| Postgres ↔ extractor | TCP em rede Compose | TLS via cert auto-assinado | TLS com cert ACM/Vault |
| MinIO API | HTTP | HTTPS via Caddy/Nginx sidecar | TLS termination LB |
| Kafka producer ↔ broker | PLAINTEXT (rede Compose) | SSL listener no broker | mTLS + SCRAM/SASL |
| Spark ↔ MinIO | s3a HTTP | s3a HTTPS | s3a com VPC endpoint |
| Trino ↔ clientes | HTTP | HTTPS | TLS + OAuth |

### 5.2 Em repouso (at-rest)

| Componente | Estado atual (S1) | Alvo |
|---|---|---|
| MinIO data | Disco do laptop não cifrado | Server-side encryption (SSE-S3) com chave em Vault |
| Postgres data | Volume Docker | LUKS no host + `pgcrypto` para colunas sensíveis |
| Kafka logs | Volume Docker | Disk encryption (mecanismo do host) |
| Backup files | N/A na S1 | GPG cifrado antes de sair do host |

### 5.3 Chaves e algoritmos

- **Hash unidirecional:** SHA-256 (justificado em [ADR 0004](./architecture/decisions/0004-pii-masking.md))
- **Criptografia simétrica:** AES-256-GCM
- **TLS:** TLS 1.2+ apenas; cipher suites modernas
- **Chave para hash de PII:** `PII_HASH_SALT` em `.env` (S1) ou Vault (prod). Rotação **anual** documentada com plano de reprocessamento.

## 6. Controle de acesso (RBAC)

### 6.1 Roles do Postgres-source

| Role | Permissões | Quem usa |
|---|---|---|
| `app_writer` | INSERT, UPDATE em `oltp.*` | Script de seed (`scripts/seed_data.sh`) |
| `app_reader` | SELECT em `oltp.*` | Extrator de Bronze |
| `postgres` (superuser) | Tudo | Apenas migrations e setup; nunca em pipelines |

> Princípio: **um pipeline nunca usa role com mais privilégios do que precisa**.

### 6.2 Trino RBAC (S2+)

Catálogos:

| Catálogo | Conexão | Visibilidade |
|---|---|---|
| `lake_bronze` | s3a → MinIO bucket bronze | Apenas role `data_engineer` (não `analyst`) — por conter PII |
| `lake_silver` | s3a → MinIO bucket silver | Roles `data_engineer` + `analyst` (sem PII direto) |
| `lake_gold` | s3a → MinIO bucket gold | Roles `data_engineer` + `analyst` + `dashboard_user` |
| `dw_gold` | jdbc → Postgres-DW | Idem `lake_gold` |

Roles do Trino (configuradas em `infra/trino/access-control.properties`):

| Role | Acesso | Quem é |
|---|---|---|
| `data_engineer` | Todos os catálogos, todas as schemas | Time de engenharia |
| `analyst` | `lake_silver`, `lake_gold`, `dw_gold` | Cientistas de dados, analistas |
| `dashboard_user` | `lake_gold`, `dw_gold` (read-only, queries pré-aprovadas) | Service account do Metabase |

### 6.3 MinIO policies (S2+)

- `bronze/*` — apenas service accounts com policy `bronze-write` (job de ingestão) e `bronze-read` (Spark Silver job)
- `silver/*`, `gold/*`, `landing/*` — policies análogas
- **Nenhum bucket público** (default + verificado via smoke test)

### 6.4 Service accounts vs usuários humanos

- **Service accounts** (de pipeline): identificados por `client_id`, sem nome humano
- **Usuários humanos** (analistas, banca): autenticação via SSO (em prod). No case, login direto no Metabase.
- Cada um tem credencial separada — nunca compartilhar.

## 7. Mascaramento e anonimização

> Detalhado em [ADR 0004](./architecture/decisions/0004-pii-masking.md). Resumo aqui.

| Coluna | Técnica | Camada onde aplica |
|---|---|---|
| `cpf` | SHA-256(salt \|\| cpf) | Bronze→Silver |
| `nome` | Faker(seed=hash_cpf) | Bronze→Silver |
| `data_nascimento` | Generalização para `year-only` | Bronze→Silver |
| `cep` | Truncamento para 3 dígitos | Bronze→Silver |
| `email`, `telefone` | Supressão (NULL) | Bronze→Silver |

**Invariante:** nenhuma linha em `silver.*` ou `gold.*` contém PII direto. Validado por smoke test que faz `EXCEPT` entre Silver e Bronze para detectar regressão.

## 8. Retenção e direito ao esquecimento

### 8.1 Política de retenção por camada

| Camada | Retenção | Justificativa |
|---|---|---|
| Bronze (com PII) | **7 dias** após mascaramento estar em Silver | Janela mínima para reprocessamento ou auditoria emergencial |
| Silver | 1 ano (rolling) | Reprocessamento histórico Gold |
| Gold | Indefinido | Não tem PII direto; é a memória analítica |

### 8.2 Direito ao esquecimento (LGPD Art. 18, VI)

Procedimento:

1. **Localização** — gerar hash do CPF do titular usando o salt vigente
2. **DELETE direcionado:**
   - `DELETE FROM oltp.paciente WHERE cpf = ?` no Postgres-source
   - `DELETE FROM bronze.oltp_paciente WHERE cpf = ?` no Delta (Bronze)
3. **Reprocessamento Silver e Gold** (a partir do snapshot da próxima carga)
4. **VACUUM Delta** (`VACUUM bronze.oltp_paciente RETAIN 0 HOURS`) — remove arquivos históricos com o CPF original
5. **Auditoria** — registrar no log `lgpd_audit` (tabela em DW): titular_id_hash, data_solicitacao, data_execucao, executor

Tempo total estimado: <24h (S2+).

### 8.3 Rotação de salt

A **rotação anual** do `PII_HASH_SALT` quebra a capacidade de identificar registros antigos por hash. Política:

1. Gerar novo salt
2. Reprocessar Silver e Gold com mascaramento usando o **novo** salt
3. Manter **temporariamente** mapping antigo→novo apenas para joins de transição (1 mês)
4. Descartar salt antigo

## 9. Segregação por camada do lake

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE (PII presente)                                           │
│  ─────────────────────                                           │
│  Acesso: data_engineer apenas                                    │
│  Bucket: minio/bronze (policy bronze-read, bronze-write)         │
│  Retenção: 7 dias após Silver propagar                            │
└─────────────────────────────────────────────────────────────────┘
              │ pipelines/common/masking.py
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER (anonimizado)                                            │
│  ─────────────────                                                │
│  Acesso: data_engineer + analyst                                  │
│  Bucket: minio/silver                                             │
│  Retenção: 1 ano                                                  │
└─────────────────────────────────────────────────────────────────┘
              │ dbt/PySpark
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD (modelo dimensional)                                        │
│  ─────────────────                                                │
│  Acesso: data_engineer + analyst + dashboard_user                 │
│  Bucket: minio/gold + Postgres-DW                                 │
│  Retenção: indefinida                                             │
└─────────────────────────────────────────────────────────────────┘
```

## 10. Gestão de segredos

### 10.1 No case (S1–S5)

- `.env` (gitignored) é a fonte de credenciais
- `.env.example` no repo lista todas as variáveis sem valor real
- Senhas geradas com `openssl rand -base64 32`
- Salt do hash de PII gerado com `python -c "import secrets; print(secrets.token_hex(32))"`

### 10.2 Em produção (slide)

- HashiCorp Vault ou AWS Secrets Manager
- Pipelines obtêm credencial via:
  1. Service account assume role (IRSA/OIDC)
  2. Role tem permissão IAM para ler 1 segredo específico
  3. Cache em memória, nunca em disco
- Rotação automática a cada 90 dias

### 10.3 O que JAMAIS comitar

| Item | Status no `.gitignore` |
|---|---|
| `.env` | ✅ |
| `*.pem`, `*.key`, `*.crt` | ✅ |
| `secrets/` | ✅ |
| Chave de service account JSON | adicionar se aparecer |
| Token Personal Access do GitHub | nunca em arquivo do repo |

## 11. Auditoria e rastreabilidade

### 11.1 Lineage automático

- **OpenLineage events** emitidos por jobs Airflow e Spark
- Backend: Marquez (S4) — UI mostra grafo input→output por dataset
- Permite responder: "qual job mascarou aquele paciente, em que data, com qual versão de masking.py?"

### 11.2 Logs estruturados

- Formato JSON em todos os pipelines
- Campos obrigatórios: `timestamp`, `level`, `pipeline_name`, `run_id`, `dataset`, `record_count`
- **Nunca** logar PII em claro (mesmo em DEBUG) — política bloqueada por code review

### 11.3 Métricas Prometheus relevantes

| Métrica | O que indica |
|---|---|
| `masking_records_total{technique}` | Volume mascarado por técnica |
| `masking_failures_total` | Falhas no mascaramento (alerta crítico) |
| `pii_in_silver_check_failed` | Sentinela: registros com PII chegaram a Silver (deve ser sempre 0) |
| `data_freshness_seconds{layer,table}` | Latência de cada tabela |
| `lgpd_erasure_requests_total` | Volume de pedidos de exclusão |

### 11.4 Alertas

- **Crítico:** `pii_in_silver_check_failed > 0` → page imediato
- **Crítico:** `masking_failures_total / masking_records_total > 0.001` → page
- **Importante:** `data_freshness_seconds > 86400` → notificação

## 12. Modelo de ameaças

### 12.1 Atores

| Ator | Capacidade | Vetor |
|---|---|---|
| **Insider curioso** | Acesso ao MinIO ou Postgres-source | Lê PII em claro do Bronze |
| **Insider mal-intencionado** | Acesso a `app_writer` | Insere registros falsos para encobrir trilha |
| **External attacker** | Sem credencial | Tenta porta exposta no laptop |
| **Supply chain** | Compromete dependência (PySUS, Faker) | Insere backdoor que vaza PII em log |
| **Acidental** | Engenheiro com mérito | Comita salt em PR |

### 12.2 Mitigações por ator

| Ator | Mitigação principal |
|---|---|
| Insider curioso | RBAC restritivo no Bronze; auditoria de acesso |
| Insider mal-intencionado | Logs imutáveis (em prod, CloudTrail/CloudWatch); separação de funções |
| External attacker | Firewall + portas não expostas externamente; Compose só publica em localhost |
| Supply chain | Pinning de versões; `pip-audit`/`safety` em CI; pinning de digest no Compose |
| Acidental | `.gitignore`; pre-commit hook (`detect-secrets`); code review obrigatório |

### 12.3 Falhas de configuração comuns que evitamos

- ❌ Buckets MinIO públicos — `mc anonymous set none` no boot
- ❌ Postgres com senha default — variável obrigatória, sem fallback
- ❌ Kafka aberto sem ACL — em prod, desabilitar `auto.create.topics`
- ❌ Trino sem authentication — `password-authenticator.properties` exigido em S2

## 13. Backup e recuperação

> No case (laptop, dev), backup é uma cópia do `data/` com `tar`. Em produção real:

- **Postgres-source/DW:** `pg_dump` diário cifrado (GPG) → S3 cross-region
- **MinIO:** versionamento ativado nos buckets + replicação cross-region
- **Kafka:** tópicos compactados + tiered storage (S3) → recuperação ponto-no-tempo
- **Marquez:** backup do Postgres + retenção 1 ano

RPO alvo (em prod): 1 hora.
RTO alvo (em prod): 4 horas.

## 14. Evolução cloud (slide)

Mapeamento direto para AWS:

| Componente do case | Equivalente AWS |
|---|---|
| `.env` | AWS Secrets Manager + IAM |
| MinIO RBAC | S3 Bucket Policies + IAM + Lake Formation |
| Postgres roles | RDS + IAM Database Auth |
| Trino RBAC | Athena + Lake Formation grants |
| OpenLineage/Marquez | DataZone + CloudTrail |
| Métricas/Alertas | CloudWatch + SNS |
| Backup | AWS Backup + S3 Replication |
| TLS interno | ACM + VPC endpoints |

Custo adicional para LGPD: <5% do total da plataforma — encryption SSE-S3 é grátis; Vault e KMS são marginais.

## 15. Defesa em banca — perguntas e respostas

| Pergunta | Resposta de 30s |
|---|---|
| Como vocês cumprem LGPD? | Anonimização irreversível na transição Bronze→Silver, política de retenção do Bronze, processo documentado de direito ao esquecimento, RBAC por camada, auditoria via lineage. |
| O que acontece se um analista pede acesso ao Bronze? | Ele recebe `analyst`, que **não** tem catálogo Bronze. Acesso requer escalonamento e justificativa documentada, e fica em log auditável. |
| Como vocês evitam reidentificação por quasi-identifiers? | Generalização de CEP (3 dígitos) e data nasc (ano), supressão de email/telefone, e plano de k-anonymity para análises municipais finas como evolução. |
| Vocês cifram o lake at-rest? | Em laptop não — o disco do dev tem FileVault/LUKS. Em prod: SSE-S3 com chave KMS gerenciada — slide cloud detalha. |
| Como vocês rotacionam credenciais? | Em prod: Vault com TTL de 90 dias e rotação automática. Salt do hash de PII rotaciona anual com janela de reprocessamento. No case (laptop) é manual e documentado. |
| O salt fica onde? | Em `.env` (dev), em Vault/Secrets Manager (prod). Acesso ao salt é o mesmo nível de acesso que ao Bronze — quem tem um, tem o outro. |
| Como vocês auditam se PII vazou? | Métrica `pii_in_silver_check_failed` no Prometheus + smoke test em CI faz `EXCEPT` entre Bronze.cpf e Silver.cpf_hash — qualquer vazamento dispara alerta. |
| Direito ao esquecimento — qual o processo? | DELETE no OLTP + DELETE no Bronze + VACUUM Delta + reprocessamento Silver/Gold. SLA <24h. Registrado em log `lgpd_audit`. |
| Por que SHA-256 e não bcrypt/Argon2? | bcrypt/Argon2 são para senhas (resistir a brute force). Hash de identificadores para anonimização não tem ameaça de brute force se o salt for secreto. SHA-256 é mais rápido em batch e suportado nativamente em Spark. |
| O que vocês fazem contra ataque de timing? | N/A — não comparamos hashes em tempo de autenticação. Comparações de igualdade no Spark não são vulneráveis a timing attack do tipo HMAC. |
| E supply chain? | Pinning de versão de toda dependência, `pip-audit` em CI, e (em prod) imagens Docker assinadas com Cosign. |
