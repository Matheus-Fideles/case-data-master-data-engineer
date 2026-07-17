"""
Diagrama 2: Fluxo de Dados Medallion — Bronze → Silver → Gold.
Executa: python3 tools/diagrams/02-fluxo-dados.py
Saída:   docs/assets/02-fluxo-dados.png
"""
import os
BASE = os.path.join(os.path.dirname(__file__), "../..")
os.chdir(BASE)
ICONS = os.path.abspath("tools/diagrams/icons")

from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.analytics import Spark, Hive, Trino
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
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

DELTA = f"{ICONS}/delta-lake.png"

with Diagram(
    "Fluxo de Dados — Arquitetura Medallion (Bronze → Silver → Gold)",
    filename="docs/assets/02-fluxo-dados",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    # ─── FONTES ──────────────────────────────────────────────────────
    with Cluster("Fontes de Dados"):
        src_api   = Storage("APIs MS\nSINAN·SIM·CNES\nPNI·IBGE REST")
        src_oltp  = PostgreSQL("Postgres OLTP\nPacientes+PII\nsnapshot diário")
        src_kafka = Kafka("Kafka\nnotif.raw\nFaker stream")

    # ─── INGESTÃO ────────────────────────────────────────────────────
    spark_in = Spark("Spark\nIngestão\nMERGE NK")

    # ─── BRONZE ──────────────────────────────────────────────────────
    with Cluster("Bronze  ·  s3a://bronze/  ·  Delta Lake ACID"):
        br_epi    = Custom("epidemiologico\ndengue·zika·chik\nPart:nu_ano·uf", DELTA)
        br_hosp   = Custom("hospitalar\nsim_obitos·cnes\nPart:ano·uf", DELTA)
        br_vac    = Custom("vacinal+geo\nvacinacao_pni\nmunicipios", DELTA)
        br_oltp   = Custom("oltp/paciente\n⚠ PII presente\nPart:snapshot_dt", DELTA)
        br_stream = Custom("streaming\natendimentos\nwatermark 1h", DELTA)

    # ─── TRANSFORMAÇÃO ───────────────────────────────────────────────
    spark_tf = Spark("Spark\nTransform\n+PII Masking")

    # ─── SILVER ──────────────────────────────────────────────────────
    with Cluster("Silver  ·  s3a://silver/  ·  Limpeza + PII Masking (ADR-004)"):
        sv_epi    = Custom("arboviroses\ndecode idade\npadroniza datas", DELTA)
        sv_hosp   = Custom("sim·cnes norm\nCID-10 decode\ngeocode", DELTA)
        sv_vac    = Custom("vacinacao_norm\nfaixa etária\nCNES enrich", DELTA)
        sv_pac    = Custom("paciente_anon\nCPF→SHA256+salt\nid_hash ✓ sem PII", DELTA)
        sv_stream = Custom("atend_norm\nvalid. schema\nDLQ→notif.dlq", DELTA)

    # ─── STAR SCHEMA ─────────────────────────────────────────────────
    spark_gd = Spark("Spark\nStar Schema\nSCD2 Postgres")

    # ─── GOLD ────────────────────────────────────────────────────────
    with Cluster("Gold  ·  s3a://gold/ + Postgres SCD2"):
        gd_fatos = Custom("Fatos (Delta)\nfato_notificacao\nfato_atendimento\nfato_vacinacao", DELTA)
        gd_dims  = Custom("Dims (Delta)\ndim_municipio\ndim_vacina", DELTA)
        gd_scd2  = PostgreSQL("Dims SCD2 (PG)\ndim_paciente\ndim_estabelecimento\ndt_inicio·is_current")

    # ─── SERVING ─────────────────────────────────────────────────────
    with Cluster("Serving Layer"):
        hms   = Hive("Hive Metastore\nthrift:9083\nCatálogo Delta")
        trino = Trino("Trino 448\n:8085\nSELECT gold.*")

    # ─── FLUXOS ──────────────────────────────────────────────────────
    src_api   >> Edge(label="REST mensal") >> spark_in
    src_oltp  >> Edge(label="JDBC diário") >> spark_in
    src_kafka >> Edge(label="Streaming") >> spark_in

    spark_in >> [br_epi, br_hosp, br_vac, br_oltp, br_stream]

    [br_epi, br_hosp, br_vac, br_oltp, br_stream] >> spark_tf

    spark_tf >> [sv_epi, sv_hosp, sv_vac, sv_pac, sv_stream]

    [sv_epi, sv_hosp, sv_vac, sv_pac, sv_stream] >> spark_gd

    spark_gd >> [gd_fatos, gd_dims, gd_scd2]

    [gd_fatos, gd_dims] >> Edge(label="cataloga") >> hms
    gd_scd2             >> Edge(label="expõe") >> trino
    hms                 >> trino
