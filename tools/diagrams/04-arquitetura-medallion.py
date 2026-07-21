"""
Diagrama 4: Arquitetura Medallion — Bronze → Silver → Gold (foco nas camadas).
Executa: python3 tools/diagrams/04-arquitetura-medallion.py
Saída:   docs/assets/04-arquitetura-medallion.png
"""
import os
BASE = os.path.join(os.path.dirname(__file__), "../..")
os.chdir(BASE)

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark, Hive, Trino
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.generic.storage import Storage
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka

GRAPH_ATTR = {
    "fontsize": "20",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "1.0",
    "nodesep": "0.6",
    "ranksep": "1.2",
    "splines": "ortho",
    "rankdir": "LR",
}
NODE_ATTR = {
    "fontsize": "11",
    "fontname": "Helvetica",
    "width": "1.4",
    "height": "1.4",
    "fixedsize": "true",
    "imagescale": "true",
}
CLUSTER_BRONZE = {"bgcolor": "#FFF3E0", "style": "rounded", "pencolor": "#E65100", "penwidth": "2"}
CLUSTER_SILVER = {"bgcolor": "#ECEFF1", "style": "rounded", "pencolor": "#455A64", "penwidth": "2"}
CLUSTER_GOLD   = {"bgcolor": "#FFFDE7", "style": "rounded", "pencolor": "#F9A825", "penwidth": "2"}

with Diagram(
    "Case Santander — Arquitetura Medallion",
    filename="docs/assets/04-arquitetura-medallion",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    with Cluster("Fontes"):
        apis  = Storage("APIs Públicas\nSINAN · SIM\nIBGE · PNI")
        kafka = Kafka("Kafka\nStream")
        oltp  = PostgreSQL("Postgres\nOLTP + PII")

    airflow = Airflow("Airflow\nOrquestrador")

    with Cluster("BRONZE  ·  s3a://bronze/\nDelta Lake · ingestão bruta", graph_attr=CLUSTER_BRONZE):
        br1 = Spark("sim_obitos\nmunicipios")
        br2 = Spark("vacinacao_pni\narboviroses")
        br3 = Spark("streaming\natendimentos")

    with Cluster("SILVER  ·  s3a://silver/\nLimpeza · PII masking SHA-256", graph_attr=CLUSTER_SILVER):
        sv1 = Spark("sim_obitos_norm\nmunicipios_norm")
        sv2 = Spark("vacinacao_norm\narrowviroses_norm")
        sv3 = Spark("atend_norm\nDLQ → notif.dlq")

    with Cluster("GOLD  ·  s3a://gold/\nStar Schema · Delta Lake", graph_attr=CLUSTER_GOLD):
        gd1 = Spark("fato_obito\nfato_notificacao")
        gd2 = Spark("dim_municipio\ndim_agravo\ndim_tempo")

    with Cluster("Serving"):
        hms   = Hive("Hive\nMetastore")
        trino = Trino("Trino 448\n:8085")

    with Cluster("Observabilidade"):
        prom    = Prometheus("Prometheus")
        grafana = Grafana("Grafana")

    [apis, kafka, oltp] >> airflow
    airflow >> Edge(color="#E65100", label="ingestão") >> [br1, br2, br3]
    br1 >> Edge(color="#455A64") >> sv1
    br2 >> Edge(color="#455A64") >> sv2
    br3 >> Edge(color="#455A64") >> sv3
    sv1 >> Edge(color="#F9A825", label="star schema") >> gd1
    sv2 >> Edge(color="#F9A825") >> gd2
    sv3 >> Edge(color="#F9A825") >> gd1
    gd1 >> hms
    gd2 >> hms
    hms >> trino
    prom >> grafana
