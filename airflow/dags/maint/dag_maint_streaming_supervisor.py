"""Supervision DAG for the Spark Structured Streaming job.

Runs every 5 minutes and checks if the SparkApplication
'streaming-atendimento-consumer' is in RUNNING state on k3s.
If not (FAILED, COMPLETED, absent), submits a new CRD.

Spec: docs/specs/airflow-dags.md — section "Streaming"
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
        """Checks SparkApplication state; submits new CRD if necessary."""
        try:
            from kubernetes import client
            from kubernetes import config as k8s_config

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
                state = app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    state = "NOT_FOUND"
                else:
                    raise

            log.info("SparkApplication %s state=%s", _APP_NAME, state)

            if state == "RUNNING":
                return "running — no action needed"

            # State is not RUNNING — submit a new CRD
            log.warning(
                "SparkApplication %s is not RUNNING (state=%s) — submitting new CRD",
                _APP_NAME,
                state,
            )

            # Remove old CRD if it exists (avoids name conflict)
            if state != "NOT_FOUND":
                try:
                    custom.delete_namespaced_custom_object(
                        group="sparkoperator.k8s.io",
                        version="v1beta2",
                        namespace=_NAMESPACE,
                        plural="sparkapplications",
                        name=_APP_NAME,
                    )
                    log.info("Old CRD removed: %s", _APP_NAME)
                except client.exceptions.ApiException:
                    pass

            from pathlib import Path

            import yaml

            manifest_path = Path(__file__).parents[4] / "k8s" / "sparkapplications" / _MANIFEST
            with open(manifest_path) as f:
                app_manifest = yaml.safe_load(f)

            custom.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=_NAMESPACE,
                plural="sparkapplications",
                body=app_manifest,
            )
            msg = f"New CRD submitted: {_APP_NAME}"
            log.info(msg)

            # Increment Prometheus counter (best-effort)
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
            log.warning("kubernetes lib not available — skipping real check")
            return "skipped (no kubernetes lib)"

    check_and_restart = PythonOperator(
        task_id="check_and_restart_streaming_consumer",
        python_callable=_check_and_restart,
        provide_context=True,
    )
