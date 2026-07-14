# ADR 0004 — Estratégia de Mascaramento de PII

- **Status:** Aceito
- **Data:** 2026-05-07
- **Decisores:** Candidato
- **Contexto do case:** Academia Santander — Engenharia de Dados

## Contexto

O enunciado é literal sobre dois requisitos cruzados:

> **Requisito 5 (Segurança de Dados):** "Garanta a segurança e confidencialidade dos dados sensíveis dos pacientes, adotando práticas de criptografia, controle de acesso e cumprindo regulamentações de proteção de dados, como a Lei Geral de Proteção de Dados (LGPD)."
>
> **Requisito 6 (Mascaramento de Dados):** "Proponha técnicas e exemplos de como mascarar dados sensíveis, garantindo a privacidade e anonimização das informações utilizadas na análise."

A camada `oltp.paciente` traz **PII verdadeiro** (CPF com checksum válido, nome, CEP, telefone, e-mail) gerado via Faker `pt_BR`. O Bronze contém esses dados em claro — propositadamente, para demonstrar a transição "raw → seguro" durante a apresentação. **A partir do Silver, ninguém pode mais ver PII em claro.**

Esta decisão registra **onde, como e por que** o mascaramento acontece, **quais técnicas** se aplicam a cada coluna sensível e **quais alternativas** foram avaliadas e rejeitadas.

## Opções avaliadas

### Opção A — UDFs PySpark aplicadas no Bronze→Silver ✅ (escolhida)

Funções puras em `pipelines/common/masking.py` aplicadas como UDFs durante a transformação Bronze→Silver. A mesma biblioteca é importada por:
- DAGs de batch (Airflow + Spark) para mascarar `oltp.paciente`
- Spark Structured Streaming para mascarar `atendimentos_stream` em tempo real
- Testes unitários (`tests/unit/test_masking.py`)

**Vantagens:**
- **Mascaramento aplicado uma única vez**, no ponto exato em que o dado deixa a camada raw
- Lógica versionada em código, testável, reproduzível
- Mesma função usada por batch e streaming → **invariante: dado em Silver+ NUNCA tem PII**
- Auditável: o `git blame` mostra quando e por quem cada técnica foi alterada
- Desacopla armazenamento (Delta) da política — qualquer migração futura preserva o tratamento

**Desvantagens (mitigadas):**
- UDFs Python têm overhead vs operações nativas → **mitigação:** preferir SQL Spark nativo (`sha2`, `concat`, `substring`) sempre que possível; UDF Python só onde inevitável (Faker)
- Salt em variável de ambiente é menos seguro que cofre — **mitigação:** documentado em [`security.md`](../../security.md) como evolução para Vault em produção

### Opção B — Views Postgres com funções PL/pgSQL — rejeitada

Criar `oltp.paciente_anonimo` como view que aplica `digest()` etc. e expor apenas a view para extração.

**Por que foi rejeitada:**
- Limita o mascaramento ao dado vindo de Postgres — precisaríamos de outra solução para o stream Kafka, que não passa por Postgres
- Lógica de mascaramento espalhada (PL/pgSQL + Python) → divergência inevitável
- Funções PL/pgSQL são chatas de testar
- Quebra o princípio de "mascaramento centralizado em código"

### Opção C — Column-level encryption no Delta Lake — rejeitada

Delta Lake suporta criptografia de colunas via Parquet Modular Encryption (PME). Cada coluna tem chave própria.

**Por que foi rejeitada:**
- PME é complexo: KMS, key rotation, política de acesso por footer
- **Reversível por design** (criptografia, não anonimização) → não atende ao requisito de **anonimização** literal do enunciado
- Footprint operacional alto para o ganho — banca pode aceitar como evolução, mas não como ponto de partida
- Em laptop com MinIO sem KMS real, vira teatro

### Opção D — Trino views com `mask_fn` — rejeitada

Trino Lake Formation policies aplicam mascaramento na hora da query.

**Por que foi rejeitada:**
- Mascaramento no momento da consulta deixa o dado **em claro** no storage (Silver) — quem tem acesso direto ao MinIO bypassa a política
- O case quer dado anonimizado **persistido**, não só visualizado anonimamente
- Útil em produção como **camada extra** sobre dado já anonimizado, não como única defesa

## Decisão

**Adotar UDFs PySpark em `pipelines/common/masking.py`** aplicadas exclusivamente na transição Bronze → Silver, com as seguintes técnicas por coluna:

| Coluna PII | Técnica | Reversível? | Determinístico? | Justificativa |
|---|---|---|---|---|
| `cpf` | **Hash SHA-256 + salt** | Não | Sim | Preserva join (mesmo CPF → mesmo hash) sem rastrear identidade |
| `nome` | **Faker pt_BR com seed = hash(cpf)** | Não | Sim | Aparência realista para análise; mesmo paciente → mesmo nome fake |
| `data_nascimento` | **Generalização: ano apenas** (`yyyy`) | Parcial | Sim | Habilita faixa etária; remove identificação fina |
| `cep` | **Truncamento: 3 primeiros dígitos** | Parcial | Sim | Análise por microrregião; perde nível de quadra |
| `email` | **Supressão (NULL)** | — | — | Sem valor analítico que justifique guardar |
| `telefone` | **Supressão (NULL)** | — | — | Idem |
| `endereco_completo` (não está no schema atual mas se entrar) | **Supressão** | — | — | Idem |

### Algoritmo de hash (defesa em banca)

```
hash_cpf = SHA-256( salt || cpf_normalizado )
salt     = variável de ambiente PII_HASH_SALT (256+ bits)
```

- **SHA-256** é escolhido por estar entre os algoritmos sugeridos pela ANPD para anonimização (junto com SHA-3 e BLAKE2)
- **Salt fixo por ambiente** (não por linha) — necessário para preservar o **determinismo do join** (`fato_internacao.sk_paciente` resolve para `dim_paciente.sk_paciente` via lookup pelo hash). Salt aleatório por linha quebraria o join e tornaria o pipeline inútil.
- **Salt por linha** seria pseudonimização forte mas inviabilizaria o caso de uso analítico — explicitamente fora do escopo

### Determinismo de Faker

```
seed = int(hash_cpf[:16], 16)   # primeiros 64 bits do hash como int
fake = Faker(locale='pt_BR')
Faker.seed(seed)
nome_fake = fake.name()
```

Mesmo CPF original → mesmo nome Faker em todas as execuções. Diferentes CPFs → nomes diferentes (com colisões raríssimas inerentes ao birthday paradox em Faker, ~0.001% para 50k pacientes — aceitável para análise).

## Consequências

### Positivas

- **Conformidade LGPD:** SHA-256 + salt é considerado anonimização aceitável pela ANPD desde que o salt não seja exposto e a chave da função hash seja gerenciada
- **Defesa direta para 4 perguntas previsíveis da banca:**
  - "Como vocês mascaram CPF?" → hash com salt secreto
  - "E se alguém tiver acesso aos dados mascarados, consegue identificar a pessoa?" → não, porque o salt é segredo e SHA-256 é unidirecional
  - "Por que SHA-256 e não MD5?" → MD5 está formalmente quebrado para colisões; SHA-256 é o mínimo industrial
  - "E se você quiser saber quantas internações um paciente teve?" → contagem é trivial: `COUNT(*) GROUP BY hash_cpf`
- **Reuso em batch e streaming** garante invariante "PII some no Silver"
- **Testável** com fixtures conhecidas (CPF do exemplo da Receita: `123.456.789-09` → hash determinístico)

### Negativas (e mitigações)

- **Timing attacks no hash:** PHP-style attacks comparando tempo de hash não se aplicam (não é login). Ignorável.
- **Salt na env:** menos seguro que Vault. Mitigação: `.env` no `.gitignore` desde Dia 1; runbook documenta rotação; produção usa Vault/Secrets Manager (slide).
- **Rotação de salt invalida joins históricos:** mudar o salt requer **reprocessamento completo do Silver e Gold**. Mitigação: política documentada de rotação anual com janela de manutenção; histórico antes da rotação fica em snapshot offline.
- **Faker pt_BR pode gerar nome real por coincidência:** birthday paradox em catálogo finito de Faker → ~0.5% de chance em 50k pacientes. Mitigação: aceitar probabilidade (estatisticamente seguro, e o nome não é ligável ao CPF original).
- **Generalização parcial não é anonimização forte:** mesmo o CEP de 3 dígitos + ano de nascimento + sexo pode permitir reidentificação em municípios pequenos. Mitigação documentada: aplicar **k-anonymity adicional no Gold** se análises por município pequeno forem necessárias (mover para SK 5+).

## Pontos de aplicação no pipeline

```
[Bronze]                    [pipelines/common/masking.py]              [Silver]
oltp.paciente   ──→   apply_paciente_masking(df)   ──→   silver.paciente_mascarado
                          │
                          ├── hash_cpf()
                          ├── fake_name(seed=hash_cpf)
                          ├── generalize_birth_year()
                          ├── truncate_cep_3()
                          ├── suppress(email, telefone)
                          └── add_meta_columns(masked_at, masking_version)


atendimentos_stream  ──→   apply_stream_masking(df)   ──→   silver.atendimentos
   (Bronze append)              │
                                ├── hash_cpf()  (mesmo salt → joina com dim_paciente)
                                └── add_meta_columns(...)
```

## Rastreabilidade e versionamento

Toda linha mascarada em Silver carrega:
- `masked_at TIMESTAMPTZ` — quando foi mascarado
- `masking_version STRING` — versão do `masking.py` que rodou (constante exportada do módulo, ex.: `"masking-1.0.0"`)

Mudança de técnica de mascaramento = nova versão = novo reprocessamento. **Linhas com versões diferentes coexistem temporariamente** durante migração; relatório de drift detecta.

## Auditoria

- **Eventos OpenLineage** emitidos em cada job de mascaramento incluem nome do dataset de entrada (Bronze) e saída (Silver), permitindo rastrear o lineage completo no Marquez
- **Métrica Prometheus** `masking_records_total{technique="hash_cpf"}` permite alertar se queda brusca (sintoma de bug que deixa PII passar)
- **Smoke test** valida que `silver.paciente_mascarado.cpf` não tem nenhum CPF que apareça no Bronze original (proteção contra regressão)

## Referências

- **ANPD — Guia de Boas Práticas LGPD:** <https://www.gov.br/anpd/pt-br/documentos-e-publicacoes/guia-de-boas-praticas-lgpd>
- **ANPD — Estudo Técnico sobre Pseudonimização e Anonimização:** <https://www.gov.br/anpd/pt-br/documentos-e-publicacoes/2024-temp/estudo-tecnico-anonimizacao>
- **NIST SP 800-188 — Trustworthy De-Identification Techniques** (referência internacional comparativa)
- **OWASP — Hashing Cheat Sheet:** <https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html> (sobre SHA-256 vs alternativas)
- **k-anonymity (Sweeney, 2002):** referência clássica para reidentificação por quasi-identifiers
- **Decisão relacionada:** [Modelo dimensional Gold](../data-model.md) define que dimensões só recebem dados pós-masking

## Apêndice — perguntas da banca + resposta-padrão

| Pergunta provável | Resposta de 30s |
|---|---|
| Por que hash em vez de criptografia? | Hash é unidirecional — anonimiza. Criptografia é reversível — pseudonimiza. LGPD pede anonimização para dados que não precisam voltar. |
| Por que SHA-256 e não SHA-3? | Por velocidade e suporte de plataforma. Spark `sha2(col, 256)` é nativo e batch-friendly. SHA-3 é equivalente em segurança mas menos suportado no ecossistema atual. |
| Como você protege o salt? | Em dev: `.env` no `.gitignore`, rotação documentada. Em prod: Vault + chave em KMS. Acesso ao salt é o mesmo nível de acesso que ao Bronze. |
| Mascarar quebra o join entre fato e dimensão? | Não — o hash é determinístico, então mesmo CPF resulta sempre no mesmo hash, e joins funcionam. |
| Reidentificação por quasi-identifiers (CEP+sexo+ano)? | Risco real. Mitigamos com **generalização** (CEP 3 dígitos, ano apenas) e documentamos `k-anonymity` como evolução para análises municipais finas. |
| E se um paciente exercer "direito ao esquecimento" da LGPD? | Documentado em [`security.md`](../../security.md): processo de localização do hash via salt + DELETE no Bronze + reprocessamento Silver/Gold. Política de retenção do Bronze: 7 dias após mascaramento. |
