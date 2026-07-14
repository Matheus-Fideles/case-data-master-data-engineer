.DEFAULT_GOAL := help
SHELL := /bin/bash

# ── Variáveis ────────────────────────────────────────────────────────────────
COMPOSE := docker compose
K8S_NAMESPACE := spark
SPARK_IMAGE := registry.rancher.com:5000/spark-custom:3.5-delta
SPARK_OPERATOR_CHART := spark-operator/spark-operator
SPARK_OPERATOR_VERSION := 1.4.6
AIRFLOW_CLI := $(COMPOSE) exec airflow-scheduler airflow

# ── Help ─────────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Mostra este help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}' \
		| sort

# ── Docker Compose ───────────────────────────────────────────────────────────
.PHONY: up-core up-streaming up-serving up-observability up-all down restart logs

up-core: ## Sobe postgres + minio + airflow (profile core)
	$(COMPOSE) --profile core up -d
	@echo "Aguardando Airflow healthy (max 90s)..."
	@until curl -sf http://localhost:8080/health 2>/dev/null | grep -q '"status":"healthy"'; do sleep 3; done
	@echo "Serviços core prontos."

up-streaming: ## Adiciona kafka + stream-producer
	$(COMPOSE) --profile streaming up -d

up-serving: ## Adiciona trino + metabase
	$(COMPOSE) --profile serving up -d

up-observability: ## Adiciona prometheus + grafana + marquez
	$(COMPOSE) --profile observability up -d

up-all: ## Sobe todos os profiles (demo completa)
	$(COMPOSE) --profile core --profile streaming --profile serving --profile observability up -d
	@echo "Aguardando Airflow healthy (max 90s)..."
	@until curl -sf http://localhost:8080/health 2>/dev/null | grep -q '"status":"healthy"'; do sleep 3; done
	@echo "Todos os serviços prontos. Rodando health check..."
	@$(MAKE) health-check

down: ## Para e remove todos os containers
	$(COMPOSE) --profile core --profile streaming --profile serving --profile observability down

restart: down up-all ## Reinicia tudo

logs: ## Tails de logs de todos os serviços
	$(COMPOSE) --profile core --profile streaming --profile serving --profile observability logs -f

# ── Health checks ─────────────────────────────────────────────────────────────
.PHONY: health-check
health-check: ## Verifica saúde de todos os serviços via curl
	@echo "=== Health check dos serviços ==="
	@curl -sf http://localhost:8080/health > /dev/null && echo "  ✓ Airflow" || echo "  ✗ Airflow"
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "  ✓ MinIO" || echo "  ✗ MinIO"
	@docker exec $$($(COMPOSE) ps -q postgres) pg_isready -U postgres > /dev/null 2>&1 && echo "  ✓ Postgres" || echo "  ✗ Postgres"
	@curl -sf http://localhost:8085/v1/info > /dev/null && echo "  ✓ Trino" || echo "  ✗ Trino"
	@curl -sf http://localhost:3001/api/health > /dev/null && echo "  ✓ Metabase" || echo "  ✗ Metabase"
	@curl -sf http://localhost:9090/-/healthy > /dev/null && echo "  ✓ Prometheus" || echo "  ✗ Prometheus"
	@curl -sf http://localhost:3000/api/health > /dev/null && echo "  ✓ Grafana" || echo "  ✗ Grafana"
	@curl -sf http://localhost:5000/api/v1/namespaces > /dev/null && echo "  ✓ Marquez" || echo "  ✗ Marquez"

# ── Smoke tests (pytest) ──────────────────────────────────────────────────────
.PHONY: smoke smoke-min

smoke: ## Roda suite completa de smoke tests (tests/smoke/)
	KEEP_COMPOSE=1 python -m pytest tests/smoke/ -v --tb=short -m smoke

smoke-min: ## Roda smoke mínimo: infra + bronze + masking + idempotência + segurança
	KEEP_COMPOSE=1 python -m pytest \
		tests/smoke/test_01_infra_health.py \
		tests/smoke/test_02_bronze_ingestion.py \
		tests/smoke/test_03_silver_masking.py \
		tests/smoke/test_06_idempotency.py \
		tests/smoke/test_08_security_invariants.py \
		-v --tb=short -m smoke

# ── Kubernetes / Spark ────────────────────────────────────────────────────────
.PHONY: k8s-check k8s-setup k8s-spark-image k8s-namespace k8s-spark-install

k8s-check: ## Verifica conectividade com Rancher Desktop
	@kubectl cluster-info --context rancher-desktop 2>&1 | head -2
	@kubectl get nodes --context rancher-desktop

k8s-namespace: ## Cria namespace spark e serviceaccount
	kubectl --context rancher-desktop apply -f k8s/namespace.yaml
	kubectl --context rancher-desktop apply -f k8s/serviceaccount.yaml

k8s-spark-install: ## Instala spark-on-k8s-operator via Helm
	helm repo add spark-operator https://kubeflow.github.io/spark-operator
	helm repo update
	helm upgrade --install spark-operator $(SPARK_OPERATOR_CHART) \
		--version $(SPARK_OPERATOR_VERSION) \
		--namespace spark \
		--create-namespace \
		--set sparkJobNamespace=spark \
		--set webhook.enable=true \
		--kube-context rancher-desktop

k8s-spark-image: ## Build + push imagem Spark customizada para registry local
	docker build -t $(SPARK_IMAGE) -f infra/spark/Dockerfile infra/spark/
	docker push $(SPARK_IMAGE)

k8s-secrets: ## Aplica secrets do MinIO e Postgres no namespace spark
	kubectl --context rancher-desktop apply -f k8s/minio-secret.yaml
	kubectl --context rancher-desktop apply -f k8s/postgres-secret.yaml

k8s-setup: k8s-namespace k8s-spark-install k8s-secrets ## Setup completo do k8s (namespace + spark-operator + secrets)
	@echo "k8s setup concluído. Verifique com: kubectl get pods -n spark"

k8s-status: ## Status dos pods Spark no k3s
	kubectl --context rancher-desktop get sparkapplications -n $(K8S_NAMESPACE)
	kubectl --context rancher-desktop get pods -n $(K8S_NAMESPACE)

# ── Airflow ──────────────────────────────────────────────────────────────────
.PHONY: airflow-setup run-demo-pipeline

airflow-setup: ## Cria conexões, variáveis e pools no Airflow
	$(AIRFLOW_CLI) connections add minio \
		--conn-type aws \
		--conn-login $${MINIO_ROOT_USER} \
		--conn-password $${MINIO_ROOT_PASSWORD} \
		--conn-extra '{"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}' || true
	$(AIRFLOW_CLI) connections add kubernetes_default \
		--conn-type kubernetes \
		--conn-extra '{"in_cluster": false, "kube_config_path": "/opt/airflow/.kube/config", "context": "rancher-desktop"}' || true
	$(AIRFLOW_CLI) variables set OFFLINE_MODE "$${OFFLINE_MODE:-0}"
	$(AIRFLOW_CLI) variables set DATA_REF_ANO "$${DATA_REF_ANO:-2024}"
	$(AIRFLOW_CLI) pools set spark_pool 4 "Slots para SparkKubernetesOperator"
	$(AIRFLOW_CLI) pools set extraction_pool 8 "Slots para extratores REST"
	$(AIRFLOW_CLI) pools set dw_load_pool 2 "Slots para carga no DW Gold"

run-demo-pipeline: ## Trigger manual do pipeline E2E (dengue → silver → gold)
	$(AIRFLOW_CLI) dags trigger dag_bronze_dengue
	$(AIRFLOW_CLI) dags trigger dag_bronze_municipios
	$(AIRFLOW_CLI) dags trigger dag_bronze_vacinacao_pni
	@echo "Pipelines iniciados. Acompanhe em http://localhost:8080"

# ── Dados ─────────────────────────────────────────────────────────────────────
.PHONY: seed seed-clean

seed: ## Baixa amostras de APIs e salva em data/raw/ (para OFFLINE_MODE=1)
	@echo "Baixando amostras de APIs do Ministério da Saúde..."
	mkdir -p data/raw/dengue data/raw/zika data/raw/chikungunya \
	         data/raw/sim data/raw/vacinacao data/raw/cnes data/raw/municipios
	python scripts/seed_data.py
	@echo "Dados salvos em data/raw/. Use OFFLINE_MODE=1 para modo offline."

seed-clean: ## Remove dados cacheados
	rm -rf data/raw/

# ── Qualidade ─────────────────────────────────────────────────────────────────
.PHONY: lint test test-unit test-smoke

lint: ## Roda pre-commit em todos os arquivos
	pre-commit run --all-files

test-unit: ## Roda testes unitários (sem infraestrutura)
	python -m pytest tests/extraction/ tests/batch/ -v

test-smoke: smoke ## Alias para smoke tests (requer docker compose up)

# ── Pre-aquecimento (antes da demo) ──────────────────────────────────────────
.PHONY: warmup

warmup: ## Pull de imagens Docker + k8s (rodar 10 min antes da demo)
	@echo "=== Pré-aquecendo imagens Docker ==="
	$(COMPOSE) --profile core --profile streaming --profile serving --profile observability pull
	@echo "=== Pré-aquecendo imagens k8s ==="
	kubectl --context rancher-desktop run _prefetch \
		--image=$(SPARK_IMAGE) \
		--restart=Never \
		--dry-run=client \
		-o yaml | kubectl --context rancher-desktop apply -f - 2>/dev/null || true
	docker pull apache/airflow:2.9.3
	docker pull trinodb/trino:448
	@echo "Warmup concluído."

# ── Limpeza ───────────────────────────────────────────────────────────────────
.PHONY: clean

clean: down seed-clean ## Remove containers, volumes e dados cacheados
	$(COMPOSE) --profile core --profile streaming --profile serving --profile observability \
		down --volumes --remove-orphans
	docker network prune -f
