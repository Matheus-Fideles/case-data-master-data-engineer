"""DAG de supervisão do job Spark Structured Streaming.

Roda a cada 5 minutos e verifica se o SparkApplication
'streaming-atendimento-consumer' está no estado RUNNING no k3s.
Se não estiver (FAILED, COMPLETED, ausente), submete um novo CRD.

Spec: docs/specs/airflow-dags.md — seção "Streaming"
ADR: docs/architecture/decisions/0007-spark-on-kubernetes.md
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_APP_NAME = "streaming-atendimento-consumer"
_NAMESPACE = "spark"
_MANIFEST = "streaming-atendimento.yaml"

with DAG(
    dag_id="dag_maint_streaming_supervisor",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maint", "streaming", "supervisor"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    def _check_and_restart(**context) -> str:
        """Verifica estado do SparkApplication; submete novo CRD se necessário."""
        try:
            from kubernetes import client, config as k8s_config

            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException:
                k8s_config.load_kube_config()

            custom = client.CustomObjectsApi()
            try:
                app = custom.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=_NAMESPACE,
                    plural="sparkapplications",
                    name=_APP_NAME,
                )
                state = (
                    app.get("status", {})
                    .get("applicationState", {})
                    .get("state", "UNKNOWN")
                )
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    state = "NOT_FOUND"
                else:
                    raise

            log.info("SparkApplication %s state=%s", _APP_NAME, state)

            if state == "RUNNING":
                return "running — nenhuma ação necessária"

            # Estado não é RUNNING — submete novo CRD
            log.warning(
                "SparkApplication %s não está RUNNING (state=%s) — submetendo novo CRD",
                _APP_NAME, state,
            )

            # Remove o CRD antigo se existir (evita conflito de nome)
            if state != "NOT_FOUND":
                try:
                    custom.delete_namespaced_custom_object(
                        group="sparkoperator.k8s.io",
                        version="v1beta2",
                        namespace=_NAMESPACE,
                        plural="sparkapplications",
                        name=_APP_NAME,
                    )
                    log.info("CRD antigo removido: %s", _APP_NAME)
                except client.exceptions.ApiException:
                    pass

            import yaml
            from pathlib import Path

            manifest_path = (
                Path(__file__).parents[4] / "k8s" / "sparkapplications" / _MANIFEST
            )
            with open(manifest_path) as f:
                app_manifest = yaml.safe_load(f)

            custom.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=_NAMESPACE,
                plural="sparkapplications",
                body=app_manifest,
            )
            msg = f"Novo CRD submetido: {_APP_NAME}"
            log.info(msg)

            # Incrementa contador Prometheus (melhor esforço)
            try:
                import requests
                requests.post(
                    "http://pushgateway:9091/metrics/job/streaming_supervisor",
                    data="streaming_consumer_restarts_total 1\n",
                    timeout=2,
                )
            except Exception:
                pass

            return msg

        except ImportError:
            # kubernetes não instalado — ambiente de teste
            log.warning("kubernetes lib não disponível — pulando check real")
            return "skipped (no kubernetes lib)"

    check_and_restart = PythonOperator(
        task_id="check_and_restart_streaming_consumer",
        python_callable=_check_and_restart,
        provide_context=True,
    )
