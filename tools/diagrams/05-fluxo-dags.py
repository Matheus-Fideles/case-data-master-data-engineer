"""
Diagrama 5: Fluxo de DAGs — dependências entre camadas do pipeline.
Executa: python3 tools/diagrams/05-fluxo-dags.py
Saída:   docs/assets/05-fluxo-dags.png
"""
import os
BASE = os.path.join(os.path.dirname(__file__), "../..")
os.chdir(BASE)

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Spark

GRAPH_ATTR = {
    "fontsize": "18",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "1.0",
    "nodesep": "0.5",
    "ranksep": "1.0",
    "splines": "ortho",
    "rankdir": "TB",
}
NODE_ATTR = {
    "fontsize": "10",
    "fontname": "Helvetica",
    "width": "1.6",
    "height": "1.0",
    "fixedsize": "true",
    "imagescale": "true",
}

BRONZE_CLUSTER = {"bgcolor": "#FFF3E0", "style": "rounded", "pencolor": "#BF360C", "penwidth": "2"}
SILVER_CLUSTER = {"bgcolor": "#ECEFF1", "style": "rounded", "pencolor": "#37474F", "penwidth": "2"}
GOLD_CLUSTER   = {"bgcolor": "#FFFDE7", "style": "rounded", "pencolor": "#F57F17", "penwidth": "2"}
QUALITY_CLUSTER = {"bgcolor": "#EDE7F6", "style": "rounded", "pencolor": "#4527A0", "penwidth": "2"}

with Diagram(
    "Case Santander — Fluxo de DAGs",
    filename="docs/assets/05-fluxo-dags",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    with Cluster("BRONZE  ·  Ingestão Bruta (Delta Lake raw)", graph_attr=BRONZE_CLUSTER):
        br_sim    = Airflow("bronze_sim_obitos")
        br_mun    = Airflow("bronze_municipios")
        br_vac    = Airflow("bronze_vacinacao_pni")
        br_notif  = Airflow("bronze_dengue\nbronze_zika\nbronze_chikungunya")

    with Cluster("SILVER  ·  Limpeza + PII Masking SHA-256", graph_attr=SILVER_CLUSTER):
        sv_sim  = Airflow("silver_sim_obitos")
        sv_mun  = Airflow("silver_municipio")
        sv_vac  = Airflow("silver_vacinacao_pni")
        sv_notif = Airflow("silver_notificacao")

    with Cluster("GOLD  ·  Star Schema (Delta Lake)", graph_attr=GOLD_CLUSTER):
        gd_dims  = Airflow("gold_dims\n(municipio·agravo\nvacina·tempo)")
        gd_fato  = Airflow("gold_fato_obito")
        gd_notif = Airflow("gold_fato_notificacao")
        gd_vac   = Airflow("gold_fato_vacinacao")

    with Cluster("QUALIDADE  ·  Great Expectations + SLOs", graph_attr=QUALITY_CLUSTER):
        qg_silver = Airflow("quality_gates_silver")
        qg_gold   = Airflow("quality_gates_gold")

    br_sim   >> Edge(color="#BF360C") >> sv_sim
    br_mun   >> Edge(color="#BF360C") >> sv_mun
    br_vac   >> Edge(color="#BF360C") >> sv_vac
    br_notif >> Edge(color="#BF360C") >> sv_notif

    sv_sim  >> Edge(color="#37474F") >> gd_fato
    sv_mun  >> Edge(color="#37474F") >> gd_dims
    sv_vac  >> Edge(color="#37474F") >> [gd_dims, gd_vac]
    sv_notif >> Edge(color="#37474F") >> gd_notif

    sv_sim  >> Edge(color="#4527A0", style="dashed") >> qg_silver
    gd_fato >> Edge(color="#4527A0", style="dashed") >> qg_gold
