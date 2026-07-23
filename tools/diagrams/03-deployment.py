"""
Diagrama 3: Deployment local — Docker Compose profiles + Rancher Desktop k3s.
Executa: python3 tools/diagrams/03-deployment.py
Saída:   docs/assets/03-deployment.png
"""
import os
BASE = os.path.join(os.path.dirname(__file__), "../..")
os.chdir(BASE)
ICONS = os.path.abspath("tools/diagrams/icons")

from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark, Hive, Trino
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.container import Docker, K3S
from diagrams.onprem.tracing import Jaeger
from diagrams.generic.storage import Storage

GRAPH_ATTR = {
    "fontsize": "18",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "0.8",
    "nodesep": "0.7",
    "ranksep": "1.3",
    "splines": "ortho",
    "rankdir": "LR",
}
NODE_ATTR = {
    "fontsize": "10",
    "fontname": "Helvetica",
    "width": "1.1",
    "height": "1.1",
    "fixedsize": "true",
    "imagescale": "true",
}

MINIO = f"{ICONS}/minio.png"

with Diagram(
    "Deployment Local — Docker Compose Profiles + Spark DockerOperator",
    filename="docs/assets/03-deployment",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    # ─── CORE ────────────────────────────────────────────────────────
    with Cluster("Perfil CORE  (make demo)"):
        redis    = Docker("Redis\n:6379\nCelery broker")
        airflow  = Airflow("Airflow 2.9\n:8080\nScheduler+Worker")
        postgres = PostgreSQL("PostgreSQL 15\n:5432\noltp·gold·airflow")
        minio    = Custom("MinIO\n:9000 S3 API\n:9001 Console", MINIO)

    # ─── STREAMING ───────────────────────────────────────────────────
    with Cluster("Perfil STREAMING  (make up-streaming)"):
        producer = Docker("Stream Producer\nFaker→Kafka\n10 evt/seg")
        kafka    = Kafka("Kafka KRaft\n:9092\nnotif.raw(3p)\nnotif.dlq(1p)")

    # ─── SERVING ─────────────────────────────────────────────────────
    with Cluster("Perfil SERVING  (make up-serving)"):
        hms   = Hive("Hive Metastore\nthrift:9083\nCatálogo Delta")
        trino = Trino("Trino 448\n:8085 UI\n:8086 JDBC")

    # ─── OBSERVABILIDADE ─────────────────────────────────────────────
    with Cluster("Perfil OBSERVABILIDADE  (make up-observability)"):
        prom    = Prometheus("Prometheus\n:9090\nretention:15d")
        grafana = Grafana("Grafana\n:3000\nDashboards·SLOs")
        marquez = Jaeger("Marquez\nOpenLineage\n:5000·:5001")

    # ─── SPARK ───────────────────────────────────────────────────────
    with Cluster("Spark — DockerOperator  (rede: lake)"):
        sp_batch  = Spark("Spark Batch\nspark-custom:3.5-delta\nlocal[2]  Delta ACID")
        sp_stream = Spark("Spark Stream\nStructured Streaming\nwatermark 1h  Delta")

    # ─── MAPEAMENTO AWS ──────────────────────────────────────────────
    with Cluster("Mapeamento 1:1 AWS"):
        aws = Storage(
            "MinIO → S3+Delta\n"
            "HMS → Glue Catalog\n"
            "Kafka → MSK Serverless\n"
            "Spark → EMR Serverless\n"
            "Airflow → MWAA\n"
            "Postgres → RDS Aurora\n"
            "Trino → Athena/EKS\n"
            "Grafana → Managed\n"
            "Marquez → DataZone"
        )

    # ─── CONEXÕES ────────────────────────────────────────────────────
    redis    >> Edge(label="task queue", style="dashed") >> airflow
    airflow  >> Edge(label="SparkK8sOp") >> k3s
    k3s      >> spark_op >> [sp_batch, sp_stream]

    producer >> Edge(label="JSON") >> kafka
    kafka    >> Edge(label="consume") >> sp_stream

    sp_batch  >> Edge(label="s3a://", style="dashed") >> minio
    sp_stream >> Edge(label="append", style="dashed") >> minio
    airflow   >> Edge(label="conn", style="dashed") >> minio
    airflow   >> Edge(label="meta", style="dashed") >> postgres

    minio >> Edge(label="Delta files") >> hms
    hms   >> Edge(label="catálogo") >> trino

    [airflow, sp_batch] >> Edge(style="dashed", color="#888888") >> prom
    prom    >> grafana
    airflow >> Edge(style="dashed", color="#888888") >> marquez
