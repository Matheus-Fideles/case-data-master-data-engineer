"""
Diagrama 3: Deployment local — Docker Compose profiles + Rancher Desktop k3s.
Executa: python3 tools/diagrams/03-deployment.py
Saída:   docs/assets/03-deployment.png
"""
import os
os.chdir(os.path.join(os.path.dirname(__file__), "../.."))

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark, Hive, Trino
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.container import Docker, K3S
from diagrams.onprem.tracing import Jaeger
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL

GRAPH_ATTR = {
    "fontsize": "22",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "1.0",
    "nodesep": "0.8",
    "ranksep": "1.4",
    "splines": "ortho",
    "rankdir": "TB",
    "newrank": "true",
}
NODE_ATTR = {
    "fontsize": "12",
    "fontname": "Helvetica",
    "width": "1.6",
    "height": "1.6",
}

with Diagram(
    "Deployment Local — Docker Compose Profiles + Rancher Desktop k3s",
    filename="docs/assets/03-deployment",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    # ─── LINHA 1: PERFIS DOCKER COMPOSE ──────────────────────────────
    with Cluster("Perfil CORE  (make demo  /  make up-core)"):
        airflow = Airflow("Apache Airflow 2.9\n:8080\nWebserver + Scheduler\n+ Celery Worker")
        postgres = PostgreSQL("PostgreSQL 15\n:5432\noltp · gold · airflow\nRBAC configurado")
        minio = SQL("MinIO\n:9000 S3 API · :9001 Console\nbronze · silver · gold\nDelta Lake OSS")
        redis = Docker("Redis\n:6379\nCelery broker\ntask queue Airflow")

    with Cluster("Perfil STREAMING  (make up-streaming)"):
        kafka = Kafka("Apache Kafka KRaft\n:9092 broker\nnotificacoes.raw (3 part.)\nnotificacoes.dlq (1 part.)")
        producer = Docker("Stream Producer\napps/streaming/producer\nFaker → Kafka\n10 eventos/seg")

    with Cluster("Perfil SERVING  (make up-serving)"):
        hms = Hive("Hive Metastore\nthrift:9083\nCatálogo Delta Lake\nhive-site.xml → MinIO")
        trino = Trino("Trino 448\n:8085 UI / :8086 JDBC\ncatalog: delta\nSQL sobre Delta Lake")

    with Cluster("Perfil OBSERVABILIDADE  (make up-observability)"):
        prometheus = Prometheus("Prometheus\n:9090\nscrape: Airflow · Spark\nretention: 15 dias")
        grafana = Grafana("Grafana\n:3000  admin/admin\nDashboards pipeline\nSLOs · alertas")
        marquez = Jaeger("Marquez  OpenLineage\n:5000 UI · :5001 API\nLineage DAG → Delta\nrastreabilidade E2E")

    # ─── RANCHER DESKTOP / K3S ───────────────────────────────────────
    with Cluster("Rancher Desktop — k3s  (make k8s-setup)"):
        k3s = K3S("k3s Control Plane\nnamespace: spark\nServiceAccount: spark\nSecret: pii-secret")
        spark_op = Spark("Spark Operator\nHelm 1.1.27\nreconcilia SparkApplication\nCRDs no cluster")

        with Cluster("SparkApplication YAMLs  (k8s/)"):
            spark_batch = Spark("Spark Batch\nDriver 2 GB\nExecutors 4 GB × 2\nGreat Expectations + DQ")
            spark_stream = Spark("Spark Streaming\ncheckpoint s3a://bronze/\nwatermark 1h\nforeachBatch → Delta")

    # ─── MAPEAMENTO AWS ──────────────────────────────────────────────
    with Cluster("Mapeamento 1:1 AWS  (sem rewrite — só config)"):
        aws_note = Storage(
            "MinIO  →  S3 + Delta OSS\n"
            "HMS  →  Glue Data Catalog\n"
            "Kafka  →  MSK Serverless\n"
            "Spark k3s  →  EMR Serverless\n"
            "Airflow  →  MWAA\n"
            "Postgres  →  RDS Aurora PG\n"
            "Trino  →  Athena / Trino EKS\n"
            "Grafana  →  Managed Grafana\n"
            "Marquez  →  DataZone"
        )

    # ─── CONEXÕES ─────────────────────────────────────────────────────
    redis >> Edge(label="task queue", style="dashed") >> airflow
    airflow >> Edge(label="SparkKubernetes\nOperator") >> k3s
    k3s >> spark_op >> [spark_batch, spark_stream]

    producer >> Edge(label="JSON eventos") >> kafka
    kafka >> Edge(label="consume\ntópico") >> spark_stream

    spark_batch >> Edge(label="s3a://\nread/write", style="dashed") >> minio
    spark_stream >> Edge(label="s3a://\nDelta append", style="dashed") >> minio
    airflow >> Edge(label="S3 + Spark\nconnections", style="dashed") >> minio
    airflow >> Edge(label="metadata\nschemas", style="dashed") >> postgres

    minio >> Edge(label="Delta files") >> hms
    hms >> Edge(label="catálogo\nTabelas Delta") >> trino

    [airflow, spark_batch] >> Edge(style="dashed", color="#888888", label="métricas") >> prometheus
    prometheus >> grafana
    airflow >> Edge(style="dashed", color="#888888", label="OpenLineage\nevents") >> marquez
