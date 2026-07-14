# Runbook — Sprint 1 (Dia 2)

Operacao do ambiente local mínimo: Postgres OLTP + MinIO + Kafka KRaft.
Versao reduzida — secoes complementares (backup, restore, observabilidade) chegam no Dia 7.

## 1. Pre-requisitos

| Requisito | Versao mínima | Como verificar |
|---|---|---|
| Docker Engine | 24+ | `docker --version` |
| Docker Compose | v2+ (plugin) | `docker compose version` |
| GNU Make | 3.81+ | `make --version` |
| Python | 3.11+ (para seed/scripts da S2) | `python3 --version` |
| RAM livre | ~3 GB para a S1 | `vm_stat` (macOS) / `free -h` (Linux) |
| Disco livre | ~5 GB | `df -h .` |

Portas que precisam estar livres no host: **5432, 9000, 9001, 9092, 29092**.
Verificar com `lsof -nP -iTCP:5432 -sTCP:LISTEN` (ajustar porta).

## 2. Subir e derrubar

```bash
cp .env.example .env       # ajusta segredos antes de qualquer coisa
make warmup                # opcional — puxa imagens previamente
make up-s1                 # sobe postgres-source + minio + kafka
make ps                    # confere status
make logs                  # segue todos os logs (Ctrl+C para sair)
make logs SVC=minio        # logs de um servico especifico

make down                  # para containers, mantem volumes
make clean                 # DESTRUTIVO — apaga volumes (pede confirmacao)
```

URLs uteis apos `make up-s1`:

- MinIO Console: http://localhost:9001 (usuario/senha vem do `.env`)
- MinIO API   : http://localhost:9000
- Postgres OLTP: `postgresql://app@localhost:5432/oltp`
- Kafka broker: `localhost:9092` (de fora do Docker) ou `kafka:9092` (de outro container)

## 3. Verificar saude dos servicos

```bash
make ps
docker inspect --format '{{.Name}} {{.State.Health.Status}}' \
    $(docker compose ps -q)
```

Validacoes manuais rapidas:

```bash
# Postgres — schema oltp criado?
docker compose exec postgres-source \
    psql -U app -d oltp -c "\dt oltp.*"

# MinIO — buckets criados pelo sidecar?
docker compose run --rm --entrypoint="" minio-init \
    sh -c "mc alias set local http://minio:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD && mc ls local"

# Kafka — broker responde?
docker compose exec kafka \
    kafka-topics --bootstrap-server localhost:9092 --list
```

Esperado: `oltp.paciente` e `oltp.estabelecimento` listados; buckets `landing/bronze/silver/gold`; Kafka retorna lista vazia (ainda sem topicos).

## 4. Top 3 troubleshooting

### 4.1 Porta ocupada no host (`bind: address already in use`)

Sintoma: `make up-s1` falha com erro de bind em `5432`, `9000` ou `9092`.

Diagnostico:

```bash
lsof -nP -iTCP:5432 -sTCP:LISTEN
```

Acoes:

1. Matar o processo conflitante (frequente: instalacao local de Postgres em macOS via Homebrew — `brew services stop postgresql`).
2. Ou trocar a porta exposta editando `.env`:
   ```bash
   POSTGRES_SOURCE_PORT=15432
   MINIO_API_PORT=19000
   KAFKA_BROKER_PORT=19092
   ```
3. `make down && make up-s1`.

### 4.2 MinIO inicia mas reporta credenciais invalidas

Sintoma: console em :9001 nao aceita login; logs do `minio-init` mostram `mc: Access Denied`.

Causa comum: `.env` foi alterado depois do volume `minio_data` ja ter sido inicializado com credenciais antigas. O MinIO grava as creds no volume na primeira subida e ignora mudancas posteriores.

Solucao:

```bash
make clean        # destroi o volume (confirme com 'apagar tudo')
make up-s1        # recria com as credenciais atuais do .env
```

Em produção isso nao acontece pois as credenciais ficariam imutaveis pos-bootstrap; em dev e o trade-off do volume nomeado.

### 4.3 Kafka nao sobe — volume KRaft corrompido

Sintoma: container `kafka` em loop de restart com mensagens tipo `The Cluster ID ... doesn't match stored clusterId Some(...)`.

Causa: o `CLUSTER_ID` no Compose foi alterado depois do volume ja ter sido formatado pelo broker antigo.

Solucao:

```bash
docker compose stop kafka
docker volume rm datamaster_kafka_data
make up-s1
```

Atencao: limpa apenas o estado do Kafka, deixando Postgres e MinIO intactos. Se quiser zerar tudo, use `make clean`.
