"""
Diagrama 2: Fluxo de Dados Medallion — Bronze → Silver → Gold.
Executa: python3 tools/diagrams/02-fluxo-dados.py
Saída:   docs/assets/02-fluxo-dados.png

Abordagem: fluxo linear LR com um representante por domínio em cada camada.
"""
import os
os.chdir(os.path.join(os.path.dirname(__file__), "../.."))

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark, Hive, Trino
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL

GRAPH_ATTR = {
    "fontsize": "22",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "1.0",
    "nodesep": "0.9",
    "ranksep": "1.5",
    "splines": "ortho",
    "concentrate": "false",
}
NODE_ATTR = {
    "fontsize": "12",
    "fontname": "Helvetica",
    "width": "1.7",
    "height": "1.7",
}

with Diagram(
    "Fluxo de Dados — Arquitetura Medallion (Bronze → Silver → Gold)",
    filename="docs/assets/02-fluxo-dados",
    outformat="png",
    direction="LR",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    show=False,
):
    # ─── FONTES ──────────────────────────────────────────────────────
    with Cluster("Fontes de Dados\n(Python extrai · ADR-008)"):
        src_api = Storage("APIs Ministério Saúde\nSINAN · SIM · CNES\nPNI · IBGE\n7 endpoints REST mensal")
        src_oltp = PostgreSQL("Postgres OLTP\nPacientes · PII\nFaker pt_BR seed\nsnapshot diário")
        src_kafka = Kafka("Apache Kafka\nnotificacoes.raw\nStream Faker\n10 eventos/seg")

    # ─── SPARK EXTRAÇÃO/INGESTÃO ─────────────────────────────────────
    spark_ingest = Spark("Apache Spark\nIngestão Batch\nMERGE por NK\n+ Spark Streaming")

    # ─── BRONZE ──────────────────────────────────────────────────────
    with Cluster("Bronze  ·  s3a://bronze/\nDelta Lake ACID · MERGE por chave natural"):
        br_epi = SQL("epidemiologico\ndengue · zika · chik\nPart: nu_ano · sg_uf")
        br_hosp = SQL("hospitalar\nsim_obitos · cnes\nPart: ano · uf")
        br_vac = SQL("vacinal + geografico\nvacinacao_pni\nmunicipios (5.570)")
        br_oltp = SQL("oltp / paciente\n⚠ PII presente\nPart: snapshot_date")
        br_stream = SQL("streaming\natendimentos_stream\nwatermark 1h · append")

    # ─── SPARK TRANSFORM ─────────────────────────────────────────────
    spark_tf = Spark("Apache Spark\nTransformação + PII\nSHA-256 + salt\nGreat Expectations")

    # ─── SILVER ──────────────────────────────────────────────────────
    with Cluster("Silver  ·  s3a://silver/\nLimpeza + PII Masking (ADR-004)"):
        sv_epi = SQL("arboviroses\ndecode nu_idade_n\npadroniza datas")
        sv_hosp = SQL("sim · cnes norm.\nCID-10 decode\ngeocode fallback")
        sv_vac = SQL("vacinacao_norm\nfaixa etária\nCNES enrich")
        sv_pac = SQL("paciente_anonimizado\nCPF → SHA-256+salt\nid_hash · ano_nasc ✓")
        sv_stream = SQL("atendimentos_norm\nvalidação schema\nDLQ → notif.dlq")

    # ─── SPARK GOLD ──────────────────────────────────────────────────
    spark_gold = Spark("Apache Spark\nStar Schema\nSCD Tipo 2 Postgres\nDelta particionado")

    # ─── GOLD ────────────────────────────────────────────────────────
    with Cluster("Gold  ·  s3a://gold/  +  Postgres SCD2"):
        gd_fatos = SQL("Fatos (Delta Lake)\nfato_notificacao\nfato_atendimento\nfato_vacinacao · Part: ano_mes")
        gd_dims = SQL("Dimensões (Delta Lake)\ndim_municipio\ndim_vacina")
        gd_scd2 = PostgreSQL("Dimensões SCD2 (Postgres)\ndim_paciente\ndim_estabelecimento\ndt_inicio · dt_fim · is_current")

    # ─── SERVING ─────────────────────────────────────────────────────
    with Cluster("Serving Layer"):
        hms = Hive("Hive Metastore\nthrift:9083\nCatálogo Delta")
        trino = Trino("Trino 448\n:8085\nSELECT FROM\ndelta.gold.*")

    # ─── FLUXOS ──────────────────────────────────────────────────────
    src_api >> Edge(label="REST mensal\nOffline: data/raw/") >> spark_ingest
    src_oltp >> Edge(label="JDBC snapshot\ndiário") >> spark_ingest
    src_kafka >> Edge(label="Spark Streaming\nconsume tópico") >> spark_ingest

    spark_ingest >> [br_epi, br_hosp, br_vac]
    src_oltp >> br_oltp
    src_kafka >> br_stream

    [br_epi, br_hosp, br_vac, br_oltp, br_stream] >> Edge(label="Spark MERGE\nDelta ACID") >> spark_tf

    spark_tf >> [sv_epi, sv_hosp, sv_vac, sv_pac, sv_stream]

    [sv_epi, sv_hosp, sv_vac, sv_pac, sv_stream] >> Edge(label="Spark\nstar schema") >> spark_gold

    spark_gold >> [gd_fatos, gd_dims, gd_scd2]

    [gd_fatos, gd_dims] >> Edge(label="registra\ncatálogo") >> hms
    gd_scd2 >> Edge(label="expõe via\nTrino") >> trino
    hms >> trino
