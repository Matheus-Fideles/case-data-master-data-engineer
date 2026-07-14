# ADR 0007 — Spark on Kubernetes (Rancher Desktop) em vez de Spark Standalone

- **Status:** Aceito
- **Data:** 2026-07-13
- **Contexto:** decidir onde executar os jobs Spark (Bronze→Silver→Gold + Structured Streaming)

## Contexto

A solução precisa de um engine de processamento distribuído para transformações em lote e streaming. A escolha é sobre *onde* o Spark roda: em containers standalone dentro do Docker Compose, ou em Kubernetes.

## Opções avaliadas

| Critério | Spark Standalone (Compose) | **Spark on k8s (Rancher Desktop)** |
|---|---|---|
| Realismo de produção | Baixo — ninguém usa standalone em prod | **Alto — k8s é o padrão de mercado** |
| Setup local | Trivial (2 containers) | Médio (Rancher Desktop + Helm) |
| Isolamento de jobs | Compartilhado entre tasks | **Pod por job — isolamento real** |
| Escalabilidade demonstrável | Apenas vertical (mais RAM/CPU no container) | **Horizontal — novos executor pods por job** |
| Gerenciamento de recursos | Manual (`SPARK_WORKER_MEMORY`) | **ResourceQuota + LimitRange do k8s** |
| Integração com Airflow | `SparkSubmitOperator` (SSH-like) | **`SparkKubernetesOperator` (CRD declarativo)** |
| Diferencial para a banca | Baixo | **Alto — demonstra domínio além do Compose** |
| Risco de demo | Baixo | Médio (mais partes, mas controlado) |

## Decisão

**Spark roda no Kubernetes provisionado pelo Rancher Desktop** (k3s local).

- `spark-on-k8s-operator` instalado via Helm no namespace `spark-operator`
- Cada job Spark é um `SparkApplication` CRD declarativo (YAML em `k8s/spark/applications/`)
- Airflow usa `SparkKubernetesOperator` montando o kubeconfig do Rancher Desktop
- Spark standalone **não entra no Docker Compose**

## Arquitetura de rede

Rancher Desktop expõe o cluster k3s na porta `6443` do host. Pods Spark acessam serviços do Compose via `host.docker.internal`:

```
Airflow (Compose) ──[kubeconfig]──→ Rancher Desktop API :6443
                                          │
                                    SparkApplication pod
                                          │
                      ┌───────────────────┼──────────────────┐
                      ↓                   ↓                   ↓
              host.docker.internal  host.docker.internal  host.docker.internal
                   :9000 (MinIO)    :5432 (Postgres)     :9092 (Kafka)
```

O `MINIO_ENDPOINT` tem valor diferente por contexto:
- Dentro do Compose (Airflow tasks Python): `http://minio:9000`
- Dentro de pods k8s (Spark jobs): `http://host.docker.internal:9000`

Essa diferença é gerenciada via variável de ambiente injetada no `SparkApplication` CRD.

## Setup reproduzível

```bash
make k8s-check        # verifica Rancher Desktop rodando + kubectl apontando
make k8s-spark-install # helm install spark-on-k8s-operator
make k8s-spark-image  # build imagem Spark custom + push para registry local
make k8s-namespace    # cria namespace spark com RBAC mínimo
```

Pré-requisito do avaliador: **Rancher Desktop instalado e rodando** (documentado no README).

## Consequências

- `docker-compose.yml` não tem `spark-master` nem `spark-worker` — mais leve
- Makefile ganha targets `k8s-*` separados dos `compose-*`
- SparkApplication YAMLs ficam em `k8s/spark/applications/` — versionados junto ao código
- Imagem Spark custom (`Dockerfile.spark`) inclui JARs: delta-core, hadoop-aws, openlineage-spark
- RAM do Rancher Desktop precisa de ≥ 8 GB alocados (configurado no Rancher Desktop Preferences)

## Defesa em banca

*"Spark standalone existe apenas para desenvolvimento unitário sem infraestrutura. Em qualquer ambiente real — cloud ou on-premises — Spark roda no Kubernetes. Usar Rancher Desktop demonstra que a solução é cloud-ready: a mesma SparkApplication YAML que roda local sobe em EKS, GKE ou AKS sem alteração de código — apenas mudando o kubeconfig."*
