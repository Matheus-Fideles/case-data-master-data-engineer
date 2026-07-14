# Padrão de Engenharia — Pre-commit Hooks

- **Status:** Aceito
- **Data:** 2026-07-13
- **Escopo:** todos os repositórios do projeto (case e produção futura)

## Princípio

Pre-commit hooks são a **primeira barreira de qualidade** do código — executam localmente antes de qualquer commit. Não substituem CI/CD, mas eliminam uma classe inteira de erros triviais sem custo de pipeline.

Três propriedades obrigatórias para um hook neste projeto:

1. **Rápido** — deve completar em < 5s para não frustrar o dev. Hooks lentos são desativados.
2. **Determinístico** — mesmo input, mesmo output. Sem estado externo.
3. **Relevante** — cada hook cobre um risco real do projeto (PII, segredos, qualidade de código).

## Hooks configurados (`.pre-commit-config.yaml`)

### 1. `ruff` — linting e formatting Python

**Ferramenta:** [`charliermarsh/ruff-pre-commit`](https://github.com/astral-sh/ruff-pre-commit)

| Stage | O que faz |
|---|---|
| `ruff` (linter) | Verifica erros de estilo, imports não usados, variáveis não definidas, complexidade ciclomática |
| `ruff-format` (formatter) | Formata código no estilo Black — idempotente, sem diff em código já formatado |

**Por que Ruff e não Flake8 + Black separados?** Ruff implementa 700+ regras do Flake8/isort/Black em Rust — 10–100× mais rápido. Um único binário, menos dependências.

**Configuração relevante (`pyproject.toml`):**
```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP"]  # PEP8 + isort + nomenclatura + modernização
ignore = ["E501"]  # linha longa: ruff-format cuida disso

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["N802"]  # nome de função em test pode ser descritivo/longo
```

### 2. `yamllint` — linting YAML

**Por que?** Todos os `SparkApplication` CRDs, `docker-compose.yml`, `values.yaml` Helm e DAGs de configuração são YAML. Erro de indentação em YAML não é detectado pelo Python linter.

**Configuração (`.yamllint.yml`):**
```yaml
extends: default
rules:
  line-length:
    max: 120          # Kubernetes manifests são verbosos
  truthy:
    allowed-values: ["true", "false", "yes", "no", "on", "off"]
  comments:
    min-spaces-from-content: 1
```

### 3. `detect-secrets` — prevenção de segredos

**Ferramenta:** [`Yelp/detect-secrets`](https://github.com/Yelp/detect-secrets)

Detecta automaticamente: tokens AWS, chaves API, senhas hardcoded, JWTs, strings de conexão com credenciais.

**Fluxo de baseline:**
```bash
detect-secrets scan > .secrets.baseline   # gera baseline na primeira vez
detect-secrets audit .secrets.baseline    # revisa falsos positivos e marca como "known"
```

O hook então compara contra o baseline — só bloqueia *novos* segredos não revisados.

**Falsos positivos comuns neste projeto:**

| Padrão | Motivo falso positivo | Ação |
|---|---|---|
| `password: "changeme"` em `.env.example` | Placeholder documentado | `# pragma: allowlist secret` |
| `MINIO_ROOT_PASSWORD=minioadmin` | Demo local sem dado real | `# pragma: allowlist secret` |
| Chave de exemplo em teste | Unit test com fixture | `# pragma: allowlist secret` |

### 4. `no-pii-in-code` — anti-PII pygrep

**Tipo:** hook local com `language: pygrep`

Detecta referências a campos PII em código Python — indício de que dados sensíveis estão sendo manipulados diretamente em vez de via camada Silver mascarada.

**Padrão monitorado:**
```
(?i)(["'])(cpf|rg|nome_completo|data_nascimento|telefone|email_pessoal)(["'])
```

**Exemplos que ativam o hook:**
```python
df.select("cpf")                  # ❌ — acessa coluna PII diretamente
print(row["nome_completo"])       # ❌ — print de PII
query = "SELECT cpf FROM ..."     # ❌ — SQL com campo PII
```

**Exemplos que não ativam:**
```python
df.select("nk_cpf_hash")          # ✅ — campo já mascarado
mask_paciente(cpf=hash_value)     # ✅ — referência à função de mascaramento
```

**Exceção legítima:** `pipelines/extraction/oltp_snapshot.py` e `pipelines/common/masking.py` precisam referenciar CPF para mascarar. Adicionar `# noqa: PII` no topo do arquivo (comentário tratado pelo hook local).

### 5. Hooks padrão `pre-commit-hooks`

```yaml
- trailing-whitespace
- end-of-file-fixer
- check-yaml
- check-json
- check-merge-conflict
- check-added-large-files (maxkb: 500)
- no-commit-to-branch (branch: [main, master])
```

`check-added-large-files` previne que datasets de teste (CSV, JSON) sejam commitados por engano — dados devem estar em `data/raw/` (gitignored) ou no MinIO.

## Instalação e uso

```bash
pip install pre-commit
pre-commit install          # registra hooks no .git/hooks/pre-commit
pre-commit install --hook-type commit-msg   # opcional: lint commit messages
pre-commit run --all-files  # executa em todos os arquivos (ideal após clone)
```

### Bypass de emergência (uso restrito)

```bash
git commit --no-verify -m "..."   # bypassa hooks
```

**Quando é aceitável:** nunca em `main`. Em branch de feature, apenas se o hook tem bug confirmado e há issue aberta. Nunca para "agilizar" — se o hook está no caminho, corrija o código.

## Relação com CI/CD

Os mesmos hooks rodam no CI como `pre-commit run --all-files` em um job separado. Isso garante que:

1. Dev não precisa confiar somente no hook local (pode ter sido bypassado)
2. PR de colaborador externo também é validado
3. As versões pinadas no `.pre-commit-config.yaml` são a fonte de verdade — CI e local são idênticos

## Defesa em banca

*"Pre-commit hooks são a implementação do princípio 'shift left' de qualidade — detectar problemas antes do commit é 100× mais barato que detectar no CI ou em produção. Os quatro hooks escolhidos cobrem os riscos mais relevantes do projeto: qualidade de código Python (Ruff), validade de manifests Kubernetes e Compose (yamllint), prevenção de vazamento de credenciais (detect-secrets) e exposição acidental de PII (hook anti-PII). Cada hook é determinístico e roda em menos de 5 segundos — não há trade-off de produtividade."*

## Referências

- [ADR 0004 — Mascaramento PII](../architecture/decisions/0004-pii-masking.md)
- [pre-commit.com](https://pre-commit.com) — documentação oficial
- [Ruff docs](https://docs.astral.sh/ruff/)
- [detect-secrets](https://github.com/Yelp/detect-secrets)
