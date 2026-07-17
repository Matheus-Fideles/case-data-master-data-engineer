"""
Diagrama 1: Arquitetura de Solução — visão geral.
Executa: python3 tools/diagrams/01-arquitetura-solucao.py
Saída:   docs/assets/01-arquitetura-solucao.png
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

MINIO  = f"{ICONS}/minio.png"
DELTA  = f"{ICONS}/delta-lake.png"

with Diagram(
    "Plataforma de Vigilância Epidemiológica — Arquitetura de Solução",
    filename="docs/assets/01-arquitetura-solucao",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    with Cluster("1. Fontes de Dados"):
        apis    = Storage("APIs MS\nSINAN·SIM·CNES\nPNI·IBGE")
        oltp    = PostgreSQL("Postgres OLTP\nPacientes PII\nFaker pt_BR")
        prod    = Docker("Stream Producer\nFaker → JSON\n10 evt/seg")

    with Cluster("2. Orquestração"):
        airflow = Airflow("Apache Airflow\n:8080\nPythonOp+SparkK8s")

    with Cluster("3. Speed Layer"):
        kafka   = Kafka("Apache Kafka\nKRaft\nnotif.raw")

    with Cluster("4. Processamento (k3s)"):
        k3s     = K3S("Rancher k3s\nspark ns")
        spark   = Spark("Apache Spark\nBatch+Stream\nGreat Expect.")

    with Cluster("5. Data Lake — MinIO (Delta Lake)"):
        minio   = Custom("MinIO\n:9000 S3 API\nbronze·silver·gold", MINIO)
        bronze  = Custom("Bronze\ns3a://bronze/\nMERGE NK · ACID", DELTA)
        silver  = Custom("Silver\ns3a://silver/\nSHA-256+salt", DELTA)
        gold    = Custom("Gold\ns3a://gold/\nStar Schema", DELTA)
        gold_pg = PostgreSQL("Gold SCD2\nPostgres\ndim_paciente")

    with Cluster("6. Serving Layer"):
        hms     = Hive("Hive Metastore\nthrift:9083")
        trino   = Trino("Trino 448\n:8085\ncatalog:delta")
        pg_ac   = PostgreSQL("Postgres RBAC\nreader·analyst\nadmin")

    with Cluster("7. Observabilidade"):
        prom    = Prometheus("Prometheus\n:9090")
        grafana = Grafana("Grafana\n:3000")
        marquez = Jaeger("Marquez\nOpenLineage\n:5000")

    # Fluxo batch
    apis    >> Edge(label="REST mensal") >> airflow
    oltp    >> Edge(label="snapshot diário") >> airflow
    airflow >> k3s >> spark

    # Fluxo streaming
    prod    >> kafka >> spark

    # Medallion
    spark  >> Edge(label="MERGE Delta") >> minio
    minio  >> bronze
    bronze >> Edge(label="transform\n+masking") >> silver
    silver >> Edge(label="star schema") >> gold
    silver >> Edge(label="SCD2") >> gold_pg

    # Serving
    gold    >> hms
    gold_pg >> hms
    hms     >> trino >> pg_ac

    # Observabilidade (dashed)
    airflow >> Edge(style="dashed", color="#888888") >> marquez
    spark   >> Edge(style="dashed", color="#888888") >> prom
    prom    >> grafana
