"""Helper para submeter jobs Spark via DockerOperator (local[2]).

Substitui SparkKubernetesOperator — mantém a assinatura de make_spark_operator()
para que os DAGs existentes não precisem de nenhuma alteração.

Pré-requisitos (provisionados por 'make demo'):
  - Imagem spark-custom:3.5-delta construída localmente (make spark-image-local)
  - apps.zip no diretório raiz do projeto (make apps-zip)
  - Docker socket montado em /var/run/docker.sock no container Airflow
  - HOST_WORK_DIR apontando para o diretório raiz do projeto no host
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import yaml
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

_TEMPLATES_DIR = Path(__file__).parents[3] / "k8s" / "sparkapplications"
_SPARK_IMAGE = os.environ.get("SPARK_LOCAL_IMAGE", "spark-custom:3.5-delta")
_NETWORK = os.environ.get("SPARK_DOCKER_NETWORK", "lake")
_HOST_WORK_DIR = os.environ.get("HOST_WORK_DIR", "")


def _load_template(template_name: str) -> dict:
    with open(_TEMPLATES_DIR / template_name) as f:
        return yaml.safe_load(f.read())


def _build_spark_command(spec: dict) -> list[str]:
    """Builds spark-submit args from the SparkApplication YAML spec."""
    main_file = spec.get("mainApplicationFile", "").replace("local://", "")
    args = spec.get("arguments", [])
    return [
        "/opt/spark/bin/spark-submit",
        "--master", "local[2]",
        "--py-files", "/opt/spark/work-dir/apps.zip",
        "--conf", "spark.sql.shuffle.partitions=4",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        main_file,
        *args,
    ]


def make_spark_operator(
    task_id: str,
    template_name: str,
    substitutions: dict,
    dag,
    namespace: str = "spark",
    kubernetes_conn_id: str = "kubernetes_default",
) -> tuple:
    """Return (submit_task, sensor_task) using DockerOperator instead of k8s.

    Preserves the original signature — DAGs don't need changes.
    submit: DockerOperator that runs spark-submit in local[2] mode.
    sensor: no-op PythonOperator (kept for DAG flow compatibility).
    """
    if "BATCH_ID" not in substitutions:
        substitutions["BATCH_ID"] = str(uuid.uuid4())[:8]

    spec = _load_template(template_name)
    raw = yaml.dump(spec)
    for key, value in substitutions.items():
        raw = raw.replace("{{ " + key + " }}", str(value))
        raw = raw.replace("{{" + key + "}}", str(value))
    app_dict = yaml.safe_load(raw)

    command = _build_spark_command(app_dict.get("spec", {}))

    mounts = []
    if _HOST_WORK_DIR:
        mounts.append(Mount(source=_HOST_WORK_DIR, target="/opt/spark/work-dir", type="bind"))

    submit = DockerOperator(
        task_id=task_id,
        image=_SPARK_IMAGE,
        command=command,
        api_version="auto",
        auto_remove=True,
        do_xcom_push=False,
        network_mode=_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        environment={
            "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", ""),
            "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "MINIO_SVC_PIPELINE_USER": os.environ.get("MINIO_SVC_PIPELINE_USER", ""),
            "MINIO_SVC_PIPELINE_PASSWORD": os.environ.get("MINIO_SVC_PIPELINE_PASSWORD", ""),
            "MINIO_ENDPOINT": "http://minio:9000",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "app"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_JDBC_URL": f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'app')}",
            "PII_SALT": os.environ.get("PII_SALT", ""),
            "OPENLINEAGE_URL": os.environ.get("OPENLINEAGE_URL", ""),
            "OPENLINEAGE_NAMESPACE": os.environ.get("OPENLINEAGE_NAMESPACE", "batch"),
        },
        dag=dag,
    )

    sensor = PythonOperator(
        task_id=f"{task_id}_sensor",
        python_callable=lambda **_: None,
        dag=dag,
    )

    submit >> sensor
    return submit, sensor
