"""Helper para submeter SparkApplication via SparkKubernetesOperator."""

from __future__ import annotations

import uuid
from pathlib import Path

import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

_TEMPLATES_DIR = Path(__file__).parents[3] / "k8s" / "sparkapplications"


def _load_template(template_name: str) -> dict:
    with open(_TEMPLATES_DIR / template_name) as f:
        return yaml.safe_load(f.read())


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

    app_name = app_dict["metadata"]["name"]

    submit = SparkKubernetesOperator(
        task_id=task_id,
        namespace=namespace,
        application_file=yaml.dump(app_dict),
        kubernetes_conn_id=kubernetes_conn_id,
        dag=dag,
    )

    sensor = SparkKubernetesSensor(
        task_id=f"{task_id}_sensor",
        namespace=namespace,
        application_name=app_name,
        kubernetes_conn_id=kubernetes_conn_id,
        attach_log=True,
        dag=dag,
    )

    submit >> sensor
    return submit, sensor
