# Integração: CNES — Cadastro Nacional de Estabelecimentos de Saúde

> **Sprint:** S2
> **Requisito coberto:** Extração via API REST (variedade de formatos)

## 1. Visão geral

O **CNES** é o cadastro oficial de todos os estabelecimentos de saúde do Brasil — hospitais, UPAs, clínicas, postos. Cada estabelecimento tem um código único de 7 dígitos chamado **código CNES**. No SIH-SUS, esse código aparece na coluna `CGC_HOSP` e é a ponte para enriquecer cada internação com nome, tipo, localização e capacidade do hospital.

**Por que essa fonte:** atende o requisito de **extração via API REST** (em vez de só CSV), agrega contexto fundamental aos fatos (nome do hospital, tem UTI, localização precisa) e demonstra a junção entre arquivo (SIH-SUS) e API (CNES) — variedade autêntica.

## 2. Onde buscar

A solução oficial é a **API de Dados Abertos do Ministério da Saúde**:

```
https://apidadosabertos.saude.gov.br/cnes
```

Sem autenticação. Sem rate limit publicado (mas tratar com etiqueta — `User-Agent` próprio, retries com backoff, batch).

### Endpoints úteis

| Endpoint | O que retorna | Uso no case |
|---|---|---|
| `/estabelecimentos` | Lista paginada de estabelecimentos | Carga dimensional inicial |
| `/estabelecimentos/{cnes}` | Detalhes de um estabelecimento específico | Hidratação on-demand de hospitais que aparecem nas internações |
| `/profissionais` | Cadastro de profissionais | Não usamos no case |
| `/equipes` | Equipes de saúde | Não usamos no case |

### Parâmetros úteis em `/estabelecimentos`

| Parâmetro | Descrição | Exemplo |
|---|---|---|
| `codigo_municipio` | Filtra por município (cód. IBGE 7 dígitos) | `3550308` (São Paulo) |
| `codigo_uf` | Filtra por UF | `35` (SP) |
| `codigo_tipo_unidade` | Tipo (hospital, UPA, etc.) | `5` (hospital geral) |
| `limit` | Tamanho da página (default 20, máx 1000) | `1000` |
| `offset` | Posição inicial da página | `0`, `1000`, `2000` |

### Exemplo de URL

```
https://apidadosabertos.saude.gov.br/cnes/estabelecimentos?codigo_municipio=3550308&limit=1000&offset=0
```

## 3. Formato de retorno

JSON com array `estabelecimentos`, cada item contendo ~50 campos. Resposta paginada (cliente decide se segue `offset` ou aceita parcial).

### Estrutura simplificada

```json
{
  "estabelecimentos": [
    {
      "codigo_cnes": "2077426",
      "codigo_unidade": "...",
      "nome_razao_social": "HOSPITAL DAS CLINICAS DA FMUSP",
      "nome_fantasia": "HC FMUSP",
      "codigo_municipio": "3550308",
      "codigo_uf": "35",
      "codigo_tipo_unidade": "5",
      "descricao_tipo_unidade": "HOSPITAL GERAL",
      "endereco": "AV DR ENEAS C AGUIAR 255",
      "numero": "255",
      "bairro": "CERQUEIRA CESAR",
      "codigo_cep": "05403-000",
      "latitude": -23.55,
      "longitude": -46.66,
      "telefone": "...",
      "email": "...",
      "leitos_existentes": 2400,
      "leitos_sus": 1800,
      "tem_uti": true,
      "data_atualizacao": "2024-12-15"
    }
  ]
}
```

## 4. Estratégia de extração

### Biblioteca recomendada: **requests** + **httpx** (assíncrono opcional)

```bash
pip install requests
# ou
pip install httpx
```

A API é simples e síncrona basta. Use `httpx` apenas se quiser paralelizar páginas (ganho marginal — a etiqueta pede pausa entre chamadas).

### Padrão de extração (semântica)

A função deve:
1. Aceitar `codigo_municipio` (opcional) ou `codigo_uf` (opcional) — se nenhum, paginar país inteiro
2. Implementar **paginação automática:** loop incrementando `offset` até resposta vazia
3. Aplicar **rate limiting client-side:** `time.sleep(0.2)` entre chamadas (5 req/s)
4. **Retry com backoff exponencial:** tenacity ou retry manual (3 tentativas: 1s, 2s, 4s)
5. **Cache da resposta bruta** em `data/raw/cnes/<scope>/<timestamp>/page_<N>.json`
6. Modo offline lê páginas cacheadas em vez de chamar API

### User-Agent e headers

Sempre setar:
```
User-Agent: case-eng-dados-santander/1.0 (matheuss.fideles@hotmail.com)
Accept: application/json
```

A presença do email no User-Agent é etiqueta de bom cidadão (permite o operador da API identificar tráfego e contatar se houver problema).

## 5. Schema dos campos a extrair

Para a `dim_estabelecimento` (Silver→Gold com SCD2):

| Campo API | Tipo | Coluna Bronze | Comentário |
|---|---|---|---|
| `codigo_cnes` | string(7) | `nk_cnes` | Chave natural — ÚNICA |
| `nome_fantasia` | string | `nome` | Display name |
| `nome_razao_social` | string | `razao_social` | Para auditoria |
| `codigo_municipio` | string(7) | `codigo_municipio_ibge` | Liga a `dim_municipio` |
| `codigo_uf` | string(2) | `codigo_uf` | Redundante com município, mas útil |
| `descricao_tipo_unidade` | string | `tipo_unidade` | Categórica |
| `tem_uti` | boolean | `tem_uti` | Importante para análise de capacidade |
| `leitos_existentes` | int | `leitos_total` | Atributo SCD2 (muda no tempo) |
| `leitos_sus` | int | `leitos_sus` | Atributo SCD2 |
| `data_atualizacao` | date | `cnes_atualizado_em` | Para detectar mudança |
| `latitude`, `longitude` | float | `latitude`, `longitude` | Geolocalização |
| `codigo_cep` | string | `cep` | Em produção, normalmente sensível — neste case OK |

> **Atenção SCD2:** `leitos_existentes`, `leitos_sus`, `tem_uti` mudam ao longo do tempo. Tratar como atributos versionados em `dim_estabelecimento` (vide [`data-model.md`](../architecture/data-model.md)).

## 6. Particionamento e idempotência

### Path no MinIO

```
s3a://bronze/cnes/snapshot_date=YYYY-MM-DD/<arquivo>.parquet
```

Não particionar por UF/município — o snapshot é sempre nacional. Usar **data do snapshot** como partição (uma partição por execução).

### Modo de escrita

- **`INSERT OVERWRITE PARTITION (snapshot_date)`** — sempre escrever snapshot completo do dia.
- Silver compara com o snapshot anterior para identificar mudanças (alimentar SCD2 da `dim_estabelecimento`).

### Chave de idempotência

- `nk_cnes` — único globalmente. Lookup determinístico no Silver.

## 7. Cache offline

```
data/raw/cnes/
└── snapshot_2024-01-15/
    ├── _meta.json              # data, escopo, total de registros
    ├── page_0001.json          # primeira página
    ├── page_0002.json
    └── ...
```

Para a S2: cachear **apenas o estado de SP** (cód. UF `35`) — ~10 mil estabelecimentos, 10 páginas, ~5 MB total. Cabe no Git sem precisar de LFS.

## 8. Mock fallback

Em CI e demo offline, ler páginas JSON de `data/raw/cnes/snapshot_*/`.

Para garantir disponibilidade mesmo se a API estiver fora:
- Manter pelo menos 1 snapshot completo commitado (`snapshot_initial`)
- Smoke test usa `snapshot_initial` (não busca da web)
- Documentar no runbook como "atualizar o snapshot": rodar uma vez online, commitar resultado

## 9. Pitfalls comuns

| Pitfall | Sintoma | Mitigação |
|---|---|---|
| Paginação infinita por bug | Loop não termina | Limite explícito de páginas (ex.: `max_pages=200`) + log a cada 10 páginas |
| API responde 200 mas vem `[]` no array | Aceitar como fim, mas verificar se é mesmo fim | Validar `total` no metadata da primeira página vs soma das páginas |
| `codigo_cnes` vem com leading zero perdido em algum cliente JSON ruim | Join futuro falha | Forçar `string(7)` com `lpad` no Bronze |
| `data_atualizacao` em formato variável | Cast falha | Cast lenient (`try_to_date`) e marcar NULL se inválido |
| API muda schema sem aviso | Nova chave aparece | Bronze faz `mergeSchema=true`; Silver promove explícito |
| Latitude/longitude `null` em hospitais antigos | Mapa quebra | Aceitar nulo; filtrar para visualização |
| API retorna 429 (rate limit) | Várias falhas | Aumentar `time.sleep`; implementar retry com header `Retry-After` |
| Endpoint mudou (deprecação) | 404 | Manter URL como ENV (`CNES_API_BASE_URL`); fácil de trocar |
| JSON pretty-printed na resposta vs minificado | Cache fica enorme | Salvar com `json.dump(..., separators=(',',':'))` |

## 10. Variação: usando o CSV oficial do CNES como alternativa

O DataSUS também publica **dump completo do CNES** mensalmente via FTP:

```
ftp://ftp.datasus.gov.br/cnes/<AAMM>/BASE_DE_DADOS_CNES_<AAMM>.zip
```

Esse ZIP traz dezenas de arquivos DBC. Se a API estiver instável ou o requisito pedir outro formato, é uma alternativa de backup.

**Recomendação:** usar a **API REST como primária** (cobre o requisito explícito de "API REST" do enunciado) e mencionar o FTP como alternativa demonstrada de robustez.

## 11. Referências

- **API Dados Abertos MS:** <https://apidadosabertos.saude.gov.br/swagger-ui/index.html>
- **Documentação CNES:** <https://cnes.datasus.gov.br/>
- **Tipos de unidade (códigos):** <https://cnes2.datasus.gov.br/Mod_Ind_Unidade.asp>
- **Open Data Brasil — Saúde:** <https://opendatasus.saude.gov.br/>
- **httpx async:** <https://www.python-httpx.org/async/>
- **tenacity (retry):** <https://github.com/jd/tenacity>
