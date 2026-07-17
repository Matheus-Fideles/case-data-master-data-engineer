"""
Diagrama 1: Arquitetura de Solução — visão geral.
Executa: python3 tools/diagrams/01-arquitetura-solucao.py
Saída:   docs/assets/01-arquitetura-solucao.png
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
    "nodesep": "1.0",
    "ranksep": "1.6",
    "splines": "ortho",
    "rankdir": "LR",
    "newrank": "true",
}
NODE_ATTR = {
    "fontsize": "13",
    "fontname": "Helvetica",
    "width": "1.6",
    "height": "1.6",
    "fixedsize": "false",
}

with Diagram(
    "Plataforma de Vigilância Epidemiológica — Arquitetura de Solução",
    filename="docs/assets/01-arquitetura-solucao",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    # ─── FONTES ───────────────────────────────────────────────────────
    with Cluster("1. Fontes de Dados"):
        apis = Storage("APIs Ministério\nda Saúde\nSINAN · SIM · CNES\nPNI · IBGE · 7 REST")
        oltp = PostgreSQL("Postgres OLTP\nPacientes (PII)\nFaker pt_BR seed")
        stream_prod = Docker("Stream Producer\nFaker → JSON\n10 eventos/seg")

    # ─── ORQUESTRAÇÃO E INGESTÃO ─────────────────────────────────────
    with Cluster("2. Orquestração (Airflow)"):
        airflow = Airflow("Apache Airflow\n:8080\nDAGs batch\nPythonOp + SparkK8sOp")

    # ─── SPEED (KAFKA) ───────────────────────────────────────────────
    with Cluster("3. Speed Layer (Kafka)"):
        kafka = Kafka("Apache Kafka\nKRaft\nnotificacoes.raw\nnotificacoes.dlq")

    # ─── PROCESSAMENTO SPARK ─────────────────────────────────────────
    with Cluster("4. Processamento (Spark on k3s)"):
        k3s = K3S("Rancher Desktop\nk3s\nnamespace: spark")
        spark = Spark("Apache Spark\nBatch + Streaming\nGreat Expectations")

    # ─── DATA LAKE ───────────────────────────────────────────────────
    with Cluster("5. Data Lake Medallion — MinIO (Delta Lake)"):
        bronze = SQL("Bronze\ns3a://bronze/\nIngestão ACID\nMERGE por NK")
        silver = SQL("Silver\ns3a://silver/\nLimpo + PII masking\nSHA-256 + salt")
        gold = SQL("Gold\ns3a://gold/\nStar Schema\nFatos + Dimensões")
        gold_pg = PostgreSQL("Gold SCD2\nPostgres\ndim_paciente\ndim_estab")

    # ─── SERVING ─────────────────────────────────────────────────────
    with Cluster("6. Serving Layer"):
        hms = Hive("Hive Metastore\nthrift:9083\nCatálogo Delta")
        trino = Trino("Trino 448\n:8085\nSQL Federado\ncatalog: delta")
        pg_rbac = PostgreSQL("Postgres RBAC\nroles: reader\nanalyst · admin")

    # ─── OBSERVABILIDADE ─────────────────────────────────────────────
    with Cluster("7. Observabilidade & Governança"):
        prometheus = Prometheus("Prometheus\n:9090\nMétricas pipeline")
        grafana = Grafana("Grafana\n:3000\nDashboards · SLOs")
        marquez = Jaeger("Marquez\nOpenLineage\n:5000 · Lineage")

    # ─── FLUXO PRINCIPAL ─────────────────────────────────────────────
    apis >> Edge(label="REST mensal\nOffline: data/raw/") >> airflow
    oltp >> Edge(label="snapshot diário\nJDBC Postgres") >> airflow
    stream_prod >> Edge(label="JSON\nevento") >> kafka

    airflow >> Edge(label="extrai\n→ data/raw/") >> k3s
    kafka >> Edge(label="Spark Streaming\nconsume tópico") >> spark
    k3s >> spark

    spark >> Edge(label="MERGE\nDelta ACID") >> bronze
    bronze >> Edge(label="Spark transform\n+ PII masking") >> silver
    silver >> Edge(label="Spark\nstar schema") >> gold
    silver >> Edge(label="SCD2\nMERGE") >> gold_pg

    gold >> Edge(label="registra\ncatálogo") >> hms
    gold_pg >> hms
    hms >> Edge(label="cataloga\ntabelas Delta") >> trino
    trino >> pg_rbac

    # ─── OBSERVABILIDADE (dashed) ────────────────────────────────────
    airflow >> Edge(style="dashed", color="#999999", label="OpenLineage") >> marquez
    spark >> Edge(style="dashed", color="#999999", label="métricas") >> prometheus
    prometheus >> grafana
