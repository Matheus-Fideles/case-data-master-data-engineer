# Integração: DataSUS — SIH-SUS e SIM

> **Sprint:** S1 (SIH-SUS amostra) + S2 (SIM completo)
> **Requisitos cobertos:** Extração (formato CSV) · Volume · Variedade

## 1. Visão geral

O **DataSUS** publica mensalmente arquivos consolidados com microdados de saúde pública. Para o case usamos dois produtos:

| Produto | Sigla | O que contém | Granularidade |
|---|---|---|---|
| Sistema de Internações Hospitalares | **SIH-SUS** | Cada AIH (Autorização de Internação Hospitalar) gerada na rede SUS | 1 linha por internação |
| Sistema de Informação sobre Mortalidade | **SIM** | Cada Declaração de Óbito (DO) registrada no país | 1 linha por óbito |

**Por que essas duas fontes:** geram volume real (centenas de MB/mês por UF), têm chave natural clara (`N_AIH`, `N_DEC`), permitem cruzamento com CNES (estabelecimento) e IBGE (município de residência), e juntas formam dois fatos do star schema (`fato_internacao`, `fato_obito`).

## 2. Onde buscar

DataSUS publica via **FTP público**, sem autenticação. Não há SLA, mas o canal é estável.

### URL base

```
ftp://ftp.datasus.gov.br/dissemin/publicos/
```

### Caminhos por produto

| Produto | Caminho | Exemplo (SP / 2024-01) |
|---|---|---|
| SIH-SUS (RD = Reduzida) | `SIHSUS/200801_/Dados/RD<UF><AAMM>.dbc` | `SIHSUS/200801_/Dados/RDSP2401.dbc` |
| SIM (DO = Declarações de Óbito) | `SIM/CID10/DORES/DO<UF><AAAA>.dbc` | `SIM/CID10/DORES/DOSP2024.dbc` |

> **Atenção:** o ano em SIH-SUS é representado em 2 dígitos (`24`), em SIM é em 4 (`2024`). Não confundir.

### Outros tipos de arquivo SIH

| Sigla | Conteúdo | Vamos usar? |
|---|---|---|
| RD | Reduzida (uma linha por AIH com tudo) | ✅ Sim — fonte primária |
| SP | Serviços Profissionais (procedimentos detalhados) | ❌ não no case |
| ER | Cancelamentos | ❌ |
| RJ | Rejeitadas | ❌ |
| CH | Caráter de internação histórico | ❌ |

## 3. Formato dos arquivos

Os arquivos são publicados em **DBC** (formato proprietário comprimido baseado no DBF). Não confundir com DBC do Microsoft Access — é compressão específica do DataSUS.

```
DBC  →  (descompactar)  →  DBF  →  (ler como tabela)  →  CSV / Parquet
```

### Encoding crítico

- **Encoding nativo:** `latin1` (ISO-8859-1)
- **Não é UTF-8.** Tentar abrir como UTF-8 quebra acentos e cidadãos com nomes acentuados.
- A biblioteca recomendada (`pysus`) lida com isso automaticamente; se for ler manualmente, sempre passar `encoding='latin1'`.

### Valores ausentes

- Campos numéricos vazios chegam como **dois espaços** `"  "`, não `NULL` nem string vazia.
- Campos de data vazios chegam como `"00000000"` ou `"        "`.
- Códigos de município numéricos podem chegar com **leading zero** removido (ex.: `"35030"` em vez de `"355030"`).

## 4. Estratégia de extração

### Biblioteca recomendada: **pysus** (PySUS)

```bash
pip install pysus
```

Vantagens:
- Cuida do download FTP (sem precisar mexer em `urllib`/`ftplib`)
- Descompacta DBC → DBF → DataFrame em uma chamada
- Já trata encoding latin1
- Mantida pela comunidade (Universidade Federal do Rio de Janeiro)
- API simples: `from pysus.online_data.SIH import download` e `from pysus.online_data.SIM import download`

### Alternativas (caso pysus tenha bug)

| Lib | Uso |
|---|---|
| **pyreaddbc** | Decompressão DBC→DBF; combinar com `simpledbf` para DBF→DataFrame |
| **read.dbc** (R) | Apenas como referência caso precise validar bytes; não usamos no case |
| **dbfread** | Leitura DBF puro (depois de descompressão manual) |

### Padrão de chamada (semântica, não código)

A função de extração deve aceitar:
- `uf` (string, 2 letras)
- `ano_mes` (string `YYYY-MM` para SIH-SUS) ou `ano` (int para SIM)
- `offline_mode` (boolean — se True, lê do cache em `data/raw/`)

Comportamento:
1. Se `offline_mode=True` ou `OFFLINE_MODE=1` no env → lê CSV cacheado
2. Senão, baixa via pysus, salva DBC bruto + CSV decodificado em `data/raw/<fonte>/<uf>/<periodo>/`
3. Retorna DataFrame Pandas (chamador converte para Spark)

## 5. Schema das colunas relevantes

### SIH-SUS (RD)

> Documento oficial completo: <https://datasus.saude.gov.br/wp-content/uploads/2019/09/RD_doc.pdf>

Colunas que vamos extrair para o Bronze (mapeamento fonte → Bronze):

| Coluna fonte | Tipo | Descrição | Coluna Bronze |
|---|---|---|---|
| `N_AIH` | string(13) | Número da AIH (chave natural única) | `nk_aih` |
| `DT_INTER` | string(8) `YYYYMMDD` | Data de internação | `dt_inter` (após cast) |
| `DT_SAIDA` | string(8) ou `00000000` | Data da alta/óbito/transferência | `dt_saida` (NULL se zeros) |
| `DIAS_PERM` | int | Dias de permanência | `dias_perm` |
| `DIAG_PRINC` | string(4) | CID-10 do diagnóstico principal | `diag_princ` |
| `DIAG_SECUN` | string(4) | CID-10 secundário (pode ser vazio) | `diag_secun` |
| `PROC_REA` | string(10) | Código do procedimento SIGTAP realizado | `proc_rea` |
| `VAL_TOT` | decimal(13,2) | Valor total da AIH | `val_tot` |
| `VAL_UTI` | decimal(13,2) | Valor de UTI | `val_uti` |
| `UTI_MES_TO` | int | Total de diárias de UTI | `uti_mes_to` |
| `CAR_INT` | char(2) | Caráter da internação (eletivo/urgência) | `car_int` |
| `MORTE` | char(1) | Indicador de óbito ('0' ou '1') | `morte` |
| `COD_IDADE` | char(1) | Unidade da idade (3=ano, 4=mês, 5=dia) | `cod_idade` |
| `IDADE` | int | Idade na unidade indicada | `idade` |
| `SEXO` | char(1) | 1=M, 3=F (note a codificação numérica) | `sexo` |
| `MUNIC_RES` | string(6) | Código IBGE 6 dígitos do município de residência | `munic_res` |
| `CGC_HOSP` | string(7) | CNES do estabelecimento (chave para join CNES) | `cgc_hosp` |
| `UF_ZI` | string(2) | UF do hospital | `uf_zi` |
| `ETNIA` | string(4) | Etnia (apenas indígenas) | `etnia` |

**Total de colunas no arquivo original:** ~140. Estamos selecionando ~20. Demais ficam **disponíveis no Bronze raw** (escrita do DataFrame inteiro), mas o Silver promove só estas para o esquema canônico.

### SIM (DO)

> Documento oficial: <https://opendatasus.saude.gov.br/dataset/sim>

| Coluna fonte | Tipo | Descrição | Coluna Bronze |
|---|---|---|---|
| `NUMERODO` | string(8) | Número da DO (chave natural) | `nk_dec_obito` |
| `DTOBITO` | string(8) | Data do óbito | `dt_obito` |
| `CAUSABAS` | string(4) | Causa básica (CID-10) | `causa_bas` |
| `IDADE` | string(3) | Idade codificada (1º dígito = unidade) | `idade_cod` |
| `SEXO` | char(1) | 1=M, 2=F, 0=ignorado | `sexo` |
| `LOCOCOR` | char(1) | Local da ocorrência (1=hospital, 2=outro) | `local_ocor` |
| `CODMUNOCOR` | string(6) | Código IBGE do município de ocorrência | `codmun_ocor` |
| `CODMUNRES` | string(6) | Código IBGE do município de residência | `codmun_res` |
| `ASSISTMED` | char(1) | Recebeu assistência médica? | `assist_med` |

## 6. Particionamento e idempotência no Bronze

### Path no MinIO

```
s3a://bronze/sih/uf=SP/ano_mes=2024-01/<arquivo>.parquet
s3a://bronze/sim/uf=SP/ano=2024/<arquivo>.parquet
```

### Modo de escrita

- **`INSERT OVERWRITE PARTITION (uf, ano_mes)`** — reprocessar mês inteiro é seguro e barato; SIH-SUS não emite correções pontuais, é sempre arquivo consolidado.
- Para SIM: idem, partição `(uf, ano)`.

### Chave de idempotência

- SIH-SUS: `nk_aih` (13 dígitos, único globalmente — banco usa essa chave para evitar duplicata até no DW)
- SIM: `nk_dec_obito` (8 dígitos)

## 7. Cache offline e versionamento

### Convenção de path

```
data/raw/sih/sp/2024-01/
├── RDSP2401.dbc          # arquivo bruto baixado (NÃO commitado se >100MB)
├── RDSP2401.csv          # versão decodificada para reprodução offline (LFS)
└── _meta.json            # checksums + data download + tamanho original
```

### Tamanhos típicos

| UF/mês | DBC bruto | CSV decodificado | Linhas |
|---|---|---|---|
| SP / 2024-01 | ~25 MB | ~80 MB | ~250 mil |
| RJ / 2024-01 | ~12 MB | ~40 MB | ~110 mil |
| AC / 2024-01 | ~600 KB | ~2 MB | ~6 mil |

### Decisão de S1: downsample

Para a S1 (decidido no plano do twin-planner), commitar amostra de **10.000 linhas determinísticas** do SP/2024-01 em `data/raw/sih/sp/2024-01/RDSP2401_sample10k.csv` via Git LFS. O CSV completo fica fora do Git (vai no `.gitignore` de `data/raw/sih/**/*_full.csv`).

Algoritmo de downsample sugerido (para implementar):
1. Ler CSV completo em pandas
2. Aplicar `df.sample(n=10000, random_state=42)` — seed fixo
3. Ordenar por `N_AIH` para reprodutibilidade visual
4. Escrever com `index=False`, `encoding='utf-8'` (já decodificado)

## 8. Mock fallback (modo OFFLINE_MODE=1)

Quando o flag estiver ativo:
- A função de extração **não tenta FTP**
- Lê direto de `data/raw/sih/<uf>/<ano_mes>/RDSP<AAMM>_sample10k.csv`
- Em CI: o LFS pull do GitHub Actions garante que o arquivo está lá
- Falha com erro claro se o cache não existir (não silenciar)

## 9. Pitfalls comuns

| Pitfall | Sintoma | Mitigação |
|---|---|---|
| Encoding errado (UTF-8) | Acentos viram `?` ou `Ã£` | Sempre passar `encoding='latin1'` na leitura, ou confiar no `pysus` |
| `MUNIC_RES` com 6 vs 7 dígitos | Join com IBGE falha | DataSUS usa 6 dígitos (sem o último de validação); IBGE Sidra usa 7. **Padronizar para 6 no Bronze**, fazer cast quando cruzar com IBGE no Silver |
| `DT_SAIDA = '00000000'` | Cast para date dá erro | Tratar como NULL: `IF(dt_saida = '00000000', NULL, to_date(dt_saida, 'yyyyMMdd'))` |
| `SEXO` codificado 1/3 | Esperar 'M'/'F' quebra dashboard | Mapear no Silver: `1→M, 3→F, 0→I (ignorado)` |
| `IDADE` em SIM combina unidade+valor | Idade chega como `"401"` (4=mês, 01=1 mês) | Decodificar: 1º char unidade, 2-3 valor |
| FTP DataSUS fora do ar (raro mas acontece) | Demo quebra | Sempre rodar com `OFFLINE_MODE=1` na demo |
| Arquivos publicados com 2-3 meses de atraso | Mês corrente não existe | Usar `2024-01` (estável) na S1; aceitar latência no roadmap |
| `CGC_HOSP` com leading zero | Join CNES falha | Normalizar para 7 chars com `lpad(cgc_hosp, 7, '0')` |
| pysus instala dependências pesadas (rpy2, etc.) em algumas versões | `pip install` lento | Pinar versão; considerar `pyreaddbc` como alternativa mais leve |

## 10. Referências

- **Documentação SIH-SUS:** <https://datasus.saude.gov.br/sistemas-em-producao/sihsus/>
- **Dicionário de dados RD:** <https://datasus.saude.gov.br/wp-content/uploads/2019/09/RD_doc.pdf>
- **Documentação SIM:** <https://opendatasus.saude.gov.br/dataset/sim>
- **PySUS GitHub:** <https://github.com/AlertaDengue/PySUS>
- **PySUS docs:** <https://pysus.readthedocs.io/>
- **Tutorial DBC→DBF (R):** <https://github.com/danicat/read.dbc>
- **CID-10 (OMS):** <https://www.who.int/standards/classifications/classification-of-diseases>
- **Tabela de UFs IBGE:** <https://www.ibge.gov.br/explica/codigos-dos-municipios.php>
