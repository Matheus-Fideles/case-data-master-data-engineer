# Data Dictionary — Camada Gold

> **Audiência: avaliadores · analistas · desenvolvedores
> **Cobertura:** todas as tabelas Gold (dimensões + fatos), todas as colunas, regra de derivação, regra de negócio.
> **Princípio:** quem ler este documento entende **o que cada coluna significa, de onde vem e como foi calculada**, sem precisar abrir o código.

## Convenções

- **Surrogate Key (SK):** chave artificial gerada na transição Silver→Gold. Tipo `BIGINT`. Prefixo `sk_`.
- **Natural Key (NK):** chave do mundo real (CPF hash, CNES, código IBGE). Tipo `STRING`. Prefixo `nk_`.
- **SCD Tipo 2:** colunas de versionamento `valid_from`, `valid_to`, `is_current`, `_attr_hash`.
- **SCD Tipo 1:** sem versionamento; sobrescrita.
- **SCD Tipo 0:** imutável.
- **Métrica aditiva:** soma faz sentido (`SUM(valor_total)`).
- **Métrica semi-aditiva:** soma faz sentido em algumas dimensões (ex.: estoque agrega no espaço, não no tempo).
- **Métrica não-aditiva:** soma não faz sentido (média, ratio).

## Mapa de tabelas Gold

| Tipo | Tabela | Tipo SCD | Cardinalidade S2 esperada |
|---|---|---|---|
| Dimensão | `dim_paciente` | **2** | 50.000 base + ~5%/ano de novas versões |
| Dimensão | `dim_estabelecimento` | **2** | 500 base + ~10%/ano de novas versões |
| Dimensão | `dim_municipio` | 1 | 5.570 |
| Dimensão | `dim_cid` | 0 | ~14.000 |
| Dimensão | `dim_procedimento` | 0 | ~5.000 |
| Dimensão | `dim_tempo` | 0 | ~36.500 (100 anos) |
| Fato | `fato_internacao` | — | 250.000/UF/mês |
| Fato | `fato_obito` | — | 70.000/UF/ano |
| Fato | `fato_atendimento_stream` | — | depende do throughput |

---

## DIMENSÕES

### `gold.dim_paciente` (SCD Tipo 2)

Representa pacientes com versionamento histórico. Cada mudança de atributo gera nova linha.

| Coluna | Tipo | Origem | Regra de derivação | Significado de negócio |
|---|---|---|---|---|
| `sk_paciente` | BIGINT | gerado | Sequência incremental | Chave artificial — referenciada por todos os fatos |
| `nk_cpf_hash` | STRING(64) | `silver.paciente_mascarado.cpf_hash` | SHA-256(salt \|\| cpf) — vide [ADR 0004](./architecture/decisions/0004-pii-masking.md) | Identifica paciente sem revelar identidade |
| `nome_mascarado` | STRING | `silver.paciente_mascarado.nome_fake` | Faker(seed=hash(cpf)) | Nome sintético para visualização — sem ligação ao real |
| `data_nascimento_ano` | INTEGER | `silver.paciente_mascarado.ano_nascimento` | year(data_nascimento) — generalização | Ano de nascimento (preserva análise por faixa etária) |
| `sexo` | CHAR(1) | `silver.paciente_mascarado.sexo` | mapeamento DataSUS: 1→M, 2→F, 3→F (SIM), 0→I | M=Masculino, F=Feminino, I=Ignorado, O=Outro |
| `cep_3dig` | STRING(3) | `silver.paciente_mascarado.cep_truncado` | substring(cep, 0, 3) | Microrregião postal — habilita análise geográfica sem identificar |
| `sk_municipio_residencia` | BIGINT | lookup | `dim_municipio.sk_municipio` via `nk_codigo_ibge_7` | Municipio de residência na época do fato |
| `valid_from` | TIMESTAMP | metadata SCD2 | Quando esta versão começou a valer (snapshot do OLTP que detectou a mudança) | Início da janela de validade |
| `valid_to` | TIMESTAMP | metadata SCD2 | Quando esta versão deixou de valer (`9999-12-31` se atual) | Fim da janela de validade |
| `is_current` | BOOLEAN | metadata SCD2 | `true` se for a versão mais recente | Filtro rápido para "estado atual" |
| `_attr_hash` | STRING(64) | derivado | SHA-256 dos atributos relevantes | Detecção de mudança real (anti-rewrite) |
| `_loaded_at` | TIMESTAMP | metadata | now() na inserção | Auditoria de quando esta versão foi inserida |

**Regras de negócio críticas:**
- Versões são **disjuntas no tempo** por `nk_cpf_hash` (ver [ADR 0006](./architecture/decisions/0006-scd2.md))
- `is_current = true` ⟺ `valid_to >= '9999-01-01'`
- Joins de fato resolvem versão correta com `event_date BETWEEN valid_from AND valid_to`

---

### `gold.dim_estabelecimento` (SCD Tipo 2)

| Coluna | Tipo | Origem | Regra | Significado |
|---|---|---|---|---|
| `sk_estabelecimento` | BIGINT | gerado | sequencial | SK |
| `nk_cnes` | STRING(7) | `silver.estabelecimento.cnes` | preservado | Código CNES nacional — chave de mercado |
| `nome` | STRING | `silver.estabelecimento.nome_fantasia` | TRIM, normalização | Nome do hospital/UPA |
| `sk_municipio` | BIGINT | lookup | `dim_municipio.sk_municipio` via `codigo_municipio_ibge` | Localização |
| `tipo_unidade` | STRING | `silver.estabelecimento.tipo_unidade` | mapeamento de código para descrição | Hospital Geral, UPA, Clínica, etc. |
| `tem_uti` | BOOLEAN | `silver.estabelecimento.tem_uti` | preservado | Capacidade UTI — atributo SCD2 (muda com reforma) |
| `leitos_existentes` | INTEGER | `silver.estabelecimento.leitos_existentes` | preservado | Capacidade total — atributo SCD2 |
| `leitos_sus` | INTEGER | `silver.estabelecimento.leitos_sus` | preservado | Capacidade SUS — atributo SCD2 |
| `latitude` | DOUBLE | `silver.estabelecimento.latitude` | preservado, NULL se inválido | Geolocalização |
| `longitude` | DOUBLE | `silver.estabelecimento.longitude` | preservado | Geolocalização |
| `valid_from`, `valid_to`, `is_current`, `_attr_hash`, `_loaded_at` | — | metadata SCD2 | (igual `dim_paciente`) | (igual) |

---

### `gold.dim_municipio` (SCD Tipo 1)

Atributos descritivos sobrescritos. Sem histórico (mudanças de nome são raras e o caso analítico não exige histórico).

| Coluna | Tipo | Origem | Regra | Significado |
|---|---|---|---|---|
| `sk_municipio` | BIGINT | gerado | sequencial | SK |
| `nk_codigo_ibge_7` | STRING(7) | `silver.municipio.codigo_ibge_7` | preservado | Código IBGE 7 dígitos (com dígito verificador) |
| `codigo_ibge_6` | STRING(6) | derivado | substring(codigo_ibge_7, 0, 6) | **Para join com DataSUS** (que usa 6 dígitos) |
| `nome` | STRING | `silver.municipio.nome` | TRIM | Nome oficial |
| `sigla_uf` | STRING(2) | `silver.municipio.sigla_uf` | preservado | UF |
| `nome_uf` | STRING | `silver.municipio.nome_uf` | preservado | Nome da UF |
| `regiao` | STRING | `silver.municipio.regiao` | preservado | Norte/Nordeste/Centro-Oeste/Sudeste/Sul |
| `mesorregiao` | STRING | `silver.municipio.mesorregiao` | preservado | Agrupamento intermediário |
| `microrregiao` | STRING | `silver.municipio.microrregiao` | preservado | Agrupamento mais fino |
| `populacao` | LONG | `silver.populacao_ibge.populacao_2024` | mais recente disponível | Habitantes (denominador para taxas) |
| `_loaded_at` | TIMESTAMP | metadata | now() | Auditoria |

---

### `gold.dim_cid` (SCD Tipo 0 — imutável)

Catálogo de doenças CID-10. Estático (mudanças geram nova versão da CID, não atualização in-place).

| Coluna | Tipo | Origem | Significado |
|---|---|---|---|
| `sk_cid` | BIGINT | gerado | SK |
| `nk_codigo_cid10` | STRING(4) | seed estático | Código CID-10 (ex.: I10, E11, J18) |
| `descricao` | STRING | seed | Descrição em português |
| `capitulo` | STRING | seed | Capítulo CID-10 (I, II, III, ...) |
| `nome_capitulo` | STRING | seed | Nome do capítulo (ex.: "Doenças do aparelho circulatório") |
| `grupo` | STRING | seed | Grupo dentro do capítulo |
| `nome_grupo` | STRING | seed | Nome do grupo |

---

### `gold.dim_procedimento` (SCD Tipo 0)

| Coluna | Tipo | Origem | Significado |
|---|---|---|---|
| `sk_procedimento` | BIGINT | gerado | SK |
| `nk_codigo_sigtap` | STRING(10) | seed SIGTAP | Código do procedimento |
| `descricao` | STRING | seed | Descrição |
| `complexidade` | STRING | seed | Baixa, Média, Alta — relevante para análise de custo |

---

### `gold.dim_tempo` (SCD Tipo 0 — gerada)

Pré-computada para 100 anos (1950–2050). Permite slicing temporal sem expressões dinâmicas.

| Coluna | Tipo | Regra | Significado |
|---|---|---|---|
| `sk_tempo` | BIGINT | `YYYYMMDD` (data como int) | SK humanamente legível |
| `data` | DATE | direto | Data |
| `ano` | INTEGER | year(data) | |
| `mes` | INTEGER | month(data) | 1-12 |
| `dia` | INTEGER | day(data) | 1-31 |
| `trimestre` | INTEGER | quarter(data) | 1-4 |
| `semestre` | INTEGER | derivado | 1-2 |
| `ano_mes` | STRING | `YYYY-MM` | Útil para slicing |
| `dia_semana` | INTEGER | dayofweek(data) | 1=Dom |
| `nome_dia_semana` | STRING | mapeamento | "Segunda-feira" |
| `feriado_nacional` | BOOLEAN | lista de feriados | Útil para análise de procedimentos eletivos vs urgência |
| `eh_fim_de_semana` | BOOLEAN | derivado | dom + sáb |
| `eh_dia_util` | BOOLEAN | derivado | not (fim_de_semana or feriado) |

---

## FATOS

### `gold.fato_internacao` — fato principal SIH-SUS

Cada linha = uma AIH (Autorização de Internação Hospitalar).

| Coluna | Tipo | Origem | Regra | Significado |
|---|---|---|---|---|
| `sk_internacao` | BIGINT | gerado | sequencial | SK do fato |
| `nk_aih` | STRING(13) | `silver.internacao.n_aih` | preservado | Número da AIH (chave natural única) |
| `sk_paciente` | BIGINT | lookup SCD2 | `dim_paciente.sk_paciente WHERE dt_inter BETWEEN valid_from AND valid_to AND nk_cpf_hash = ?` | Versão do paciente vigente na admissão |
| `sk_estabelecimento` | BIGINT | lookup SCD2 | idem com `nk_cnes = cgc_hosp` e dt_inter | Hospital na época da internação |
| `sk_municipio_paciente` | BIGINT | lookup | `dim_municipio` via `munic_res` (após lpad) | Município de residência declarado |
| `sk_municipio_hospital` | BIGINT | lookup | `dim_municipio` via município do hospital | Onde a internação fisicamente aconteceu |
| `sk_cid_principal` | BIGINT | lookup | `dim_cid` via `diag_princ` | Diagnóstico principal |
| `sk_cid_secundario` | BIGINT | lookup | `dim_cid` via `diag_secun` (NULL se vazio) | Diagnóstico secundário |
| `sk_procedimento` | BIGINT | lookup | `dim_procedimento` via `proc_rea` | Procedimento realizado |
| `sk_tempo_admissao` | BIGINT | lookup | `dim_tempo` via `dt_inter` | Data de admissão |
| `sk_tempo_alta` | BIGINT | lookup | `dim_tempo` via `dt_saida` (NULL se em curso) | Data de alta/transferência/óbito |
| `dias_permanencia` | INTEGER | `silver.internacao.dias_perm` | preservado | Métrica aditiva |
| `valor_total` | DECIMAL(13,2) | `silver.internacao.val_tot` | preservado | Métrica aditiva |
| `valor_uti` | DECIMAL(13,2) | `silver.internacao.val_uti` | preservado | Métrica aditiva |
| `qtd_diarias_uti` | INTEGER | `silver.internacao.uti_mes_to` | preservado | Métrica aditiva |
| `carater_internacao` | CHAR(1) | `silver.internacao.car_int` | preservado | E=eletiva, U=urgência |
| `desfecho` | CHAR(1) | derivado | A=alta, O=óbito, T=transferência (de `morte` + `cobranca`) | Resultado da internação |
| `idade_anos` | INTEGER | derivado | de `cod_idade` + `idade` (decodificação) | Idade em anos no momento da admissão |
| `ano_mes_part` | STRING | partição | year-month de `dt_inter` | Coluna de particionamento |
| `_loaded_at` | TIMESTAMP | metadata | | Auditoria |
| `_silver_run_id` | STRING | metadata | run_id do Silver | Lineage manual de fallback |

**Métricas derivadas (views Trino, opcionais):**

| View | Cálculo | Significado |
|---|---|---|
| `permanencia_media` | `AVG(dias_permanencia) GROUP BY ...` | Tempo médio |
| `taxa_uti` | `SUM(qtd_diarias_uti)/SUM(dias_permanencia)` | Intensidade de uso de UTI |
| `taxa_obito` | `COUNT(desfecho='O')/COUNT(*)` | Letalidade |
| `internacoes_por_100k_hab` | `COUNT(*) / SUM(dim_municipio.populacao) * 100000` | Indicador epidemiológico |

---

### `gold.fato_obito` — fato SIM

Cada linha = uma Declaração de Óbito.

| Coluna | Tipo | Origem | Regra | Significado |
|---|---|---|---|---|
| `sk_obito` | BIGINT | gerado | | SK |
| `nk_dec_obito` | STRING(8) | `silver.obito.numero_do` | preservado | Número da DO |
| `sk_paciente` | BIGINT | lookup SCD2 | (cuidado: SIM nem sempre identifica via CPF, pode ficar NULL) | Paciente vinculado se identificável |
| `sk_municipio_ocorrencia` | BIGINT | lookup | `dim_municipio` via `codmun_ocor` | Onde ocorreu o óbito |
| `sk_municipio_residencia` | BIGINT | lookup | `dim_municipio` via `codmun_res` | Onde residia |
| `sk_cid_causa_basica` | BIGINT | lookup | `dim_cid` via `causa_bas` | Causa básica |
| `sk_tempo_obito` | BIGINT | lookup | `dim_tempo` via `dt_obito` | Quando |
| `idade_obito_anos` | INTEGER | derivado | Decodificação SIM | Idade em anos |
| `tipo_local_obito` | STRING | derivado | de `local_ocor` (1→hospital, 2→outro estab. saúde, 3→domicílio, 4→via pública, ...) | Local categórico |
| `recebeu_assistencia` | BOOLEAN | derivado | de `assist_med` | Recebeu assistência médica? |
| `ano_part` | INTEGER | partição | year(dt_obito) | Particionamento |
| `_loaded_at` | TIMESTAMP | metadata | | |

**Métricas derivadas:**
- `taxa_mortalidade_por_100k_hab`
- `mortalidade_por_capitulo_cid` (capítulo via dim_cid)
- `idade_mediana_obito_por_municipio`

---

### `gold.fato_atendimento_stream` — fato streaming

Cada linha = um evento de atendimento (PS, ambulatorial, etc.).

| Coluna | Tipo | Origem | Regra | Significado |
|---|---|---|---|---|
| `sk_atendimento` | BIGINT | gerado | hash(id_atendimento) | SK |
| `nk_id_atendimento` | STRING(36) | `silver.atendimento.id_atendimento` | preservado | UUID do evento |
| `sk_paciente` | BIGINT | lookup CURRENT (não SCD2 — vide ADR 0006) | versão atual de `dim_paciente` | Paciente |
| `sk_estabelecimento` | BIGINT | lookup CURRENT | idem | Hospital |
| `sk_tempo_atendimento` | BIGINT | lookup | via `ts_evento` | Quando ocorreu |
| `ts_evento` | TIMESTAMP | `silver.atendimento.ts_evento` | preservado | Event time (UTC) |
| `ts_ingestao` | TIMESTAMP | `silver.atendimento._ingested_at` | preservado | Processing time |
| `latencia_ingestao_seconds` | INTEGER | derivado | `ts_ingestao - ts_evento` | Métrica de pipeline |
| `tipo_atendimento` | STRING | `silver.atendimento.tipo_atendimento` | enum | PS, consulta, internação_aberta, urgência |
| `triagem` | STRING | `silver.atendimento.triagem` | Manchester | Verde, Amarela, Vermelha |
| `qtd_sintomas_cid` | INTEGER | derivado | `size(sintomas_cid)` | Multimorbidade |
| `cid_principal` | STRING | derivado | `sintomas_cid[0]` | Primeiro CID |
| `ano_mes_dia_part` | DATE | partição | date(ts_evento) | Particionamento |
| `_loaded_at` | TIMESTAMP | metadata | | |

---

## Views/queries de referência

A seguir, queries que viram **dashboards Metabase** ou **demos ao vivo**:

### Top municípios por taxa de internação por 100k

```sql
SELECT
  m.nome AS municipio,
  m.sigla_uf AS uf,
  COUNT(*) AS internacoes,
  m.populacao AS habitantes,
  CAST(COUNT(*) AS DOUBLE) / m.populacao * 100000 AS taxa_por_100k
FROM gold.fato_internacao f
JOIN gold.dim_municipio m ON f.sk_municipio_paciente = m.sk_municipio
JOIN gold.dim_tempo t ON f.sk_tempo_admissao = t.sk_tempo
WHERE t.ano_mes = '2024-01'
GROUP BY m.nome, m.sigla_uf, m.populacao
HAVING m.populacao > 50000
ORDER BY taxa_por_100k DESC
LIMIT 20
```

### Permanência média por capítulo CID e tipo de internação

```sql
SELECT
  c.nome_capitulo,
  f.carater_internacao,
  AVG(f.dias_permanencia) AS permanencia_media,
  COUNT(*) AS qtd_internacoes
FROM gold.fato_internacao f
JOIN gold.dim_cid c ON f.sk_cid_principal = c.sk_cid
WHERE f.dias_permanencia IS NOT NULL
GROUP BY c.nome_capitulo, f.carater_internacao
ORDER BY permanencia_media DESC
```

### Atendimentos em tempo real por estabelecimento (streaming)

```sql
SELECT
  e.nome AS hospital,
  COUNT(*) AS atendimentos_ultima_hora
FROM gold.fato_atendimento_stream a
JOIN gold.dim_estabelecimento e ON a.sk_estabelecimento = e.sk_estabelecimento AND e.is_current = true
WHERE a.ts_evento >= now() - INTERVAL '1' HOUR
GROUP BY e.nome
ORDER BY atendimentos_ultima_hora DESC
```

### Cohort de pacientes por faixa etária

```sql
SELECT
  CASE
    WHEN f.idade_anos < 18 THEN '0-17'
    WHEN f.idade_anos < 60 THEN '18-59'
    ELSE '60+'
  END AS faixa_etaria,
  COUNT(*) AS internacoes,
  AVG(f.dias_permanencia) AS permanencia_media,
  SUM(f.valor_total) AS gasto_total
FROM gold.fato_internacao f
JOIN gold.dim_tempo t ON f.sk_tempo_admissao = t.sk_tempo
WHERE t.ano = 2024
GROUP BY 1
ORDER BY 1
```

### Pacientes que mudaram de cidade (validação SCD2)

```sql
SELECT
  nk_cpf_hash,
  COUNT(*) AS qtd_versoes,
  MIN(valid_from) AS primeira_versao,
  MAX(valid_to) AS ultima_validade
FROM gold.dim_paciente
GROUP BY nk_cpf_hash
HAVING COUNT(*) > 1
ORDER BY qtd_versoes DESC
LIMIT 20
```

---

## FAQ Técnico por coluna

Algumas colunas merecem defesa explícita:

| Coluna | Pergunta provável | Resposta |
|---|---|---|
| `dim_paciente.nk_cpf_hash` | Por que hash em vez de CPF? | LGPD — anonimização irreversível, preserva join determinístico (vide ADR 0004). |
| `dim_paciente.cep_3dig` | Por que só 3 dígitos? | Generalização para microrregião — habilita análise geográfica sem identificar quadra. |
| `dim_municipio.codigo_ibge_6` | Por que duas colunas de código? | DataSUS usa 6 dígitos (sem dígito verificador), IBGE/CNES usam 7. Ambos disponíveis para join correto. |
| `fato_internacao.sk_paciente` | Como você garante a versão correta do SCD2? | Lookup com `BETWEEN valid_from AND valid_to` na data da admissão. Versões SCD2 são disjuntas, então join retorna no máximo uma linha. |
| `fato_internacao.desfecho` | Como vocês derivam desfecho? | Combinação de `MORTE` (1=óbito) + `COBRANCA` (transferência). Documentado em [`integrations/datasus.md`](./integrations/datasus.md). |
| `fato_atendimento_stream.sk_paciente` | Por que CURRENT lookup, não SCD2? | Stream é tempo real — atualização SCD2 é batch diário. Janela de divergência <24h aceitável neste caso de uso (vide ADR 0006). |
| `dim_paciente.is_current` | Por que essa coluna? | Acelera queries "estado atual" (`WHERE is_current = true`) sem precisar comparar `valid_to` com timestamp futuro. Redundante mas barato. |
| `_attr_hash` | Para que serve? | Detecta mudança real entre snapshots. Se hash igual, MERGE vira NO-OP — economia de I/O. |

## Como manter este documento

- **Quando atualizar:** sempre que adicionar/remover/renomear coluna no Gold
- **Quem atualiza:** quem fez o PR que muda o schema
- **Como validar:** code review + smoke test que confere schema do Gold contra o que está aqui descrito
- **Versão:** rastreada via `git log docs/data-dictionary.md`

## Referências

- **Spec dos schemas Bronze (origem):** [`specs/bronze-schemas.md`](./specs/bronze-schemas.md)
- **Modelo dimensional (visão de alto nível):** [`architecture/data-model.md`](./architecture/data-model.md)
- **ADR 0006 SCD2** (semântica das versões): [`architecture/decisions/0006-scd2.md`](./architecture/decisions/0006-scd2.md)
- **Documentação SIGTAP:** <https://sigtap.datasus.gov.br/tabela-unificada/app/sec/inicio.jsp>
- **CID-10 capítulos:** <https://www.datasus.gov.br/cid10/V2008/>
