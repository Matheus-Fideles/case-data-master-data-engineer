# Integração: IBGE Sidra — População e Geografia Municipal

> **Sprint:** S2
> **Requisito coberto:** Extração via API REST · Variedade de fontes

## 1. Visão geral

O **Sidra** é o portal de dados agregados do IBGE (Sistema IBGE de Recuperação Automática). Expõe uma **API REST pública** com séries demográficas, censitárias e econômicas em granularidade até nível municipal.

Para o case usamos:
- **População estimada por município** (agregado 6579) — base para calcular **taxas de internação por 100 mil habitantes**, métrica clássica em saúde pública
- **Hierarquia regional** (regiões geográficas, UFs, mesorregiões, microrregiões, municípios) — alimenta `dim_municipio`

**Por que essa fonte:** transforma contagens absolutas em **indicadores comparáveis** (o indicador a métrica `internacoes_por_100k_hab` aparece no dashboard, em vez de contagens absolutas.

## 2. Onde buscar

```
https://servicodados.ibge.gov.br/api/v3/agregados
```

Sem autenticação. Sem rate limit publicado, mas a etiqueta IBGE é parecida com a do CNES (User-Agent identificado, batch razoável).

### Estrutura geral da API

```
/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}?localidades={localidades}
```

### Agregados úteis para o case

| Agregado | O que tem | Variável principal | Períodos |
|---|---|---|---|
| **6579** | Estimativas anuais de população (TCU) | `9324` (população residente) | 2018 a 2024 |
| **4709** | Censo Demográfico 2022 — população | `93` | `2022` |
| **6579** vs 4709 | 6579 é **estimativa anual** (cobre 2018–2024); 4709 é **censo decenal** (2022) | — | Use 6579 para séries; 4709 para validação |

### Localidades (parâmetro `localidades`)

Sintaxe peculiar — vale aprender:

| Sintaxe | Significado |
|---|---|
| `BR` ou `N1[1]` | Brasil |
| `N3[35]` | UF de SP |
| `N6[3550308]` | Município de SP |
| `N6[all]` | Todos os municípios |
| `N6[all]\|N3[35]` | Todos municípios da UF SP (pipe URL-encoded `%7C`) |
| `N6[3550308,3304557]` | Lista de municípios |

> **Atenção:** o IBGE usa **7 dígitos** no código de município. O DataSUS usa **6 dígitos** (sem o último de validação). Padronizar a forma compatível **no Silver** quando cruzar as duas fontes.

### Exemplo de URL completa

População de todos os municípios em 2024:
```
https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2024/variaveis/9324?localidades=N6[all]
```

População da UF de SP em 2024:
```
https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2024/variaveis/9324?localidades=N3[35]
```

### Hierarquia geográfica (auxiliar)

Endpoint separado retorna a árvore de localidades:
```
https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome
```

Use isso para popular `dim_municipio` com nome, UF, região, mesorregião, microrregião.

## 3. Formato de retorno

JSON aninhado. A estrutura é estável mas verbosa.

### Exemplo abreviado (população 2024 de SP município)

```json
[
  {
    "id": "9324",
    "variavel": "População residente estimada",
    "unidade": "Pessoas",
    "resultados": [
      {
        "classificacoes": [],
        "series": [
          {
            "localidade": {
              "id": "3550308",
              "nivel": { "id": "N6", "nome": "Município" },
              "nome": "São Paulo - SP"
            },
            "serie": {
              "2024": "11451999"
            }
          }
        ]
      }
    ]
  }
]
```

Note que o valor (`"11451999"`) vem como **string**, não int — é convenção do Sidra. **Sempre cast para int** no Bronze→Silver, com tratamento para `"-"` (sinal de "não disponível").

## 4. Estratégia de extração

### Biblioteca recomendada: **sidrapy** (wrapper amigável)

```bash
pip install sidrapy
```

Vantagens:
- Esconde a estrutura JSON aninhada — retorna DataFrame plano
- Lida com a sintaxe peculiar de `localidades`
- Exemplo: `sidrapy.get_table(table_code='6579', territorial_level='6', ibge_territorial_code='all', period='2024', variable='9324')` retorna DataFrame com colunas `(D1C, D1N, D2C, D2N, V)` — `V` é o valor.

### Alternativa: **requests** direto

Se preferir não adicionar dep, `requests.get(url)` + `response.json()` cobre 100%. O custo é parsear a estrutura aninhada manualmente.

### Padrão de extração (semântica)

Para o case precisamos:
1. **Snapshot da população 2024 por município** (~5570 municípios) → `dim_municipio.populacao`
2. **Tabela de localidades** (hierarquia) → `dim_municipio.{nome, uf, regiao, mesorregiao, microrregiao}`

A função deve:
- Aceitar `ano` (default 2024) e `nivel_geografico` (default `N6` = municípios)
- Cachear a resposta bruta JSON em `data/raw/ibge/populacao/<ano>/all.json`
- Normalizar para DataFrame com colunas `(codigo_ibge_7, nome, populacao, uf, regiao)`
- Modo offline lê do cache

## 5. Schema dos campos a extrair

### Para `dim_municipio`

| Campo origem | Tipo | Coluna Silver | Origem |
|---|---|---|---|
| `localidade.id` | string(7) | `nk_codigo_ibge_7` | API Sidra |
| `localidade.nome` | string | `nome_uf_concat` | API Sidra (vem como "São Paulo - SP") |
| `serie.{ano}` | string→int | `populacao` | API Sidra |
| (derivado) | string(2) | `uf_sigla` | Split de `nome_uf_concat` |
| (lookup) | string | `regiao` | API Localidades |
| (lookup) | string | `mesorregiao` | API Localidades |
| (lookup) | string | `microrregiao` | API Localidades |

## 6. Particionamento e idempotência

### Path no MinIO

```
s3a://bronze/ibge/populacao/ano=2024/<arquivo>.parquet
s3a://bronze/ibge/localidades/snapshot_date=YYYY-MM-DD/<arquivo>.parquet
```

### Modo de escrita

- População: `INSERT OVERWRITE PARTITION (ano)` — uma partição por ano de estimativa
- Localidades: `INSERT OVERWRITE PARTITION (snapshot_date)` — atualizado raramente, manter histórico

### Chave de idempotência

- `(codigo_ibge_7, ano)` para população
- `codigo_ibge_7` para localidades (snapshot mais recente vence)

## 7. Cache offline

```
data/raw/ibge/
├── populacao/
│   └── 2024/
│       ├── all.json
│       └── _meta.json
└── localidades/
    └── snapshot_2024-01-15/
        ├── municipios.json
        └── _meta.json
```

Tamanho: `populacao/2024/all.json` ~1 MB; `localidades/.../municipios.json` ~3 MB. Não precisa LFS.

## 8. Mock fallback

Trivial: o JSON cacheado é a fixture. Smoke test e demo offline leem de `data/raw/ibge/` sem chamar API.

Manter pelo menos 1 ano de população em cache (`2024`). Se quiser série temporal para análise, adicionar 2018–2023 também.

## 9. Pitfalls comuns

| Pitfall | Sintoma | Mitigação |
|---|---|---|
| Código IBGE 6 vs 7 dígitos | Join com SIH-SUS falha | Padronizar **no Silver**: o lookup converte 6→7 ou trunca 7→6 |
| Valor `"-"` em série (não disponível) | Cast int falha | Tratar como NULL: `IF(serie = '-', NULL, CAST(serie AS BIGINT))` |
| Nome vem como `"São Paulo - SP"` | Quer só "São Paulo" | Split por ` - ` no Silver |
| Sintaxe `\|` em URL não escapado | API retorna 400 | URL-encode (`%7C`) ou use parâmetros separados na lib |
| Sidrapy versão antiga quebra com Python 3.12 | ImportError | Pinar `sidrapy==0.1.5` |
| API responde mas com `agregado` desatualizado | Dado de 2022 quando esperava 2024 | Sempre incluir `periodos` explícito; nunca usar `-1` (default) |
| Nivel geográfico não bate com expectativa | `N6` vs `N7` (distritos) | Verificar `nivel.id` no resultado |

## 10. Indicadores que podemos derivar (para o dashboard)

Com a `dim_municipio.populacao`, viram triviais no Gold:

| Indicador | Fórmula |
|---|---|
| Taxa de internação | `internacoes / populacao * 100000` |
| Taxa de mortalidade | `obitos / populacao * 100000` |
| Internações por leito SUS | `internacoes / leitos_sus` |
| Tempo médio de permanência por município | `avg(dias_perm) GROUP BY codigo_municipio` |

## 11. Referências

- **Documentação Sidra API:** <https://servicodados.ibge.gov.br/api/docs/agregados?versao=3>
- **Catálogo de agregados:** <https://sidra.ibge.gov.br/>
- **Hierarquia de localidades:** <https://servicodados.ibge.gov.br/api/docs/localidades?versao=1>
- **sidrapy GitHub:** <https://github.com/AlanTaranti/sidrapy>
- **Códigos de municípios IBGE:** <https://www.ibge.gov.br/explica/codigos-dos-municipios.php>
