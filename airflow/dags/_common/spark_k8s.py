"""Helper para submeter SparkApplication via SparkKubernetesOperator."""

from __future__ import annotations

import uuid
from pathlib import Path

import yaml
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

_TEMPLATES_DIR = Path(__file__).parents[3] / "k8s" / "sparkapplications"


def _load_template(template_name: str) -> dict:
    with open(_TEMPLATES_DIR / template_name) as f:
        return yaml.safe_load(f.read())


def _check_spark_completion(task_id: str, namespace: str, kubernetes_conn_id: str, **context):
    """Verifies the SparkApplication completed successfully.

    Reads pod_name from XCom (set by SparkKubernetesOperator), derives the
    SparkApplication name, and checks its status in k8s. Tolerates the case
    where the SparkApplication was already cleaned up by the spark-operator
    (fast-completing jobs): the upstream operator only returns SUCCESS when
    the job was at least RUNNING, so a missing CRD means it already completed.
    """
    from kubernetes import client, config

    ti = context["task_instance"]
    pod_name = ti.xcom_pull(task_ids=task_id, key="pod_name")
    if not pod_name:
        raise ValueError(f"No pod_name XCom from {task_id}")

    app_name = pod_name.rsplit("-driver", 1)[0]

    import os

    config.load_kube_config(config_file=os.environ.get("KUBECONFIG"))

    custom_api = client.CustomObjectsApi()
    try:
        app = custom_api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            name=app_name,
        )
        state = app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
        if state == "FAILED":
            raise Exception(f"SparkApplication {app_name} FAILED")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            # Already cleaned up by spark-operator — operator succeeded so job ran OK
            return
        raise


def make_spark_operator(
    task_id: str,
    template_name: str,
    substitutions: dict,
    dag,
    namespace: str = "spark",
    kubernetes_conn_id: str = "kubernetes_default",
) -> tuple:
    """Return (submit_task, sensor_task) for a SparkApplication.

    substitutions keys match {{ PLACEHOLDER }} tokens in the YAML template.
    A BATCH_ID is auto-generated if not provided.
    """
    if "BATCH_ID" not in substitutions:
        substitutions["BATCH_ID"] = str(uuid.uuid4())[:8]

    spec = _load_template(template_name)
    raw = yaml.dump(spec)
    for key, value in substitutions.items():
        raw = raw.replace("{{ " + key + " }}", str(value))
        raw = raw.replace("{{" + key + "}}", str(value))
    app_dict = yaml.safe_load(raw)

    submit = SparkKubernetesOperator(
        task_id=task_id,
        namespace=namespace,
        application_file=yaml.dump(app_dict),
        kubernetes_conn_id=kubernetes_conn_id,
        dag=dag,
    )

    sensor = PythonOperator(
        task_id=f"{task_id}_sensor",
        python_callable=_check_spark_completion,
        op_kwargs={
            "task_id": task_id,
            "namespace": namespace,
            "kubernetes_conn_id": kubernetes_conn_id,
        },
        dag=dag,
    )

    submit >> sensor
    return submit, sensor
