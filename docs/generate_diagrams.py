"""Gera diagramas de arquitetura do Case Santander com a lib diagrams."""

from diagrams import Cluster, Diagram, Edge
from diagrams.k8s.compute import Pod
from diagrams.onprem.analytics import Metabase, Spark, Trino
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.queue import Kafka
from diagrams.onprem.storage import Ceph as MinIO
from diagrams.onprem.tracing import Jaeger as Marquez
from diagrams.onprem.workflow import Airflow

OUTDIR = "assets"

# ── Diagrama 1: Arquitetura Medallion ────────────────────────────────────────

with Diagram(
    "Case Santander — Arquitetura Medallion",
    filename=f"{OUTDIR}/04-arquitetura-medallion",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr={"bgcolor": "#0f172a", "fontcolor": "#f8fafc", "fontname": "Helvetica"},
    node_attr={"fontname": "Helvetica", "fontsize": "11"},
    edge_attr={"color": "#94a3b8", "fontcolor": "#94a3b8", "fontname": "Helvetica"},
):
    with Cluster(
        "Fontes de Dados",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        from diagrams.programming.language import Python as PythonSrc

        apis = PythonSrc("DataSUS APIs\n(SINAN/PNI/SIM/IBGE)")
        oltp = PostgreSQL("OLTP\nPostgres 16")
        stream_src = Kafka("Kafka Events\n(atendimentos)")

    with Cluster(
        "Orquestração",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        airflow = Airflow("Airflow 2.9")

    with Cluster(
        "Processamento — Spark on k8s",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        spark = Spark("Spark 3.5\n+ Delta Lake 3.1")
        pod = Pod("SparkApplication\n(CRD k3s)")

    with Cluster(
        "Data Lake — MinIO (Delta Lake)",
        graph_attr={"bgcolor": "#0c4a6e", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        with Cluster(
            "Bronze — Raw",
            graph_attr={"bgcolor": "#164e63", "fontcolor": "#f8fafc"},
        ):
            bronze = MinIO("dengue · zika · chikungunya\nmunicipios · vacinacao · sim_obitos")

        with Cluster(
            "Silver — PII Masked",
            graph_attr={"bgcolor": "#134e4a", "fontcolor": "#f8fafc"},
        ):
            silver = MinIO("notificacao · municipio\nvacinacao_pni · sim_obitos")

        with Cluster(
            "Gold — Star Schema",
            graph_attr={"bgcolor": "#14532d", "fontcolor": "#f8fafc"},
        ):
            gold = MinIO(
                "dims: agravo · tempo · municipio · vacina\nfatos: notificacao · obito · vacinacao"
            )

    with Cluster(
        "Serving Layer",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        hive = Trino("Hive Metastore\n(HMS 4.0)")
        trino = Trino("Trino 448")
        metabase = Metabase("Metabase v0.51")

    with Cluster(
        "Observabilidade",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#f8fafc", "style": "rounded"},
    ):
        prom = Prometheus("Prometheus")
        grafana = Grafana("Grafana")
        marquez = Marquez("Marquez\n(OpenLineage)")

    apis >> Edge(label="OFFLINE_MODE=1\nextract") >> airflow
    oltp >> airflow
    stream_src >> spark

    airflow >> Edge(label="SparkKubernetes\nOperator") >> pod
    pod >> spark

    spark >> Edge(label="replaceWhere\nidempotente") >> bronze
    bronze >> Edge(label="SHA-256 + salt\nLGPD") >> silver
    silver >> silver
    silver >> Edge(label="star schema") >> gold

    gold >> hive >> trino >> metabase
    airflow >> Edge(label="OpenLineage") >> marquez
    spark >> prom >> grafana


print("✓ 04-arquitetura-medallion.png gerado")


# ── Diagrama 2: Fluxo de DAGs ────────────────────────────────────────────────

with Diagram(
    "Case Santander — Fluxo de DAGs",
    filename=f"{OUTDIR}/05-fluxo-dags",
    outformat="png",
    show=False,
    direction="TB",
    graph_attr={"bgcolor": "#0f172a", "fontcolor": "#f8fafc", "fontname": "Helvetica"},
    node_attr={"fontname": "Helvetica", "fontsize": "10"},
    edge_attr={"color": "#64748b", "fontname": "Helvetica"},
):
    with Cluster(
        "Bronze DAGs  (Delta Lake raw)",
        graph_attr={"bgcolor": "#1e3a5f", "fontcolor": "#f8fafc"},
    ):
        b_dengue = Airflow("bronze_dengue\n@monthly")
        b_zika = Airflow("bronze_zika\n@monthly")
        b_chik = Airflow("bronze_chikungunya\n@monthly")
        b_mun = Airflow("bronze_municipios\n@monthly")
        b_vac = Airflow("bronze_vacinacao_pni\n@monthly")
        b_sim = Airflow("bronze_sim_obitos\n@annually")

    with Cluster(
        "Silver DAGs  (PII masking SHA-256)",
        graph_attr={"bgcolor": "#134e4a", "fontcolor": "#f8fafc"},
    ):
        s_not = Airflow("silver_notificacao\n(dengue→zika→chik)")
        s_mun = Airflow("silver_municipio")
        s_vac = Airflow("silver_vacinacao_pni")
        s_sim = Airflow("silver_sim_obitos")

    with Cluster(
        "Gold DAGs  (Star Schema Delta)",
        graph_attr={"bgcolor": "#14532d", "fontcolor": "#f8fafc"},
    ):
        g_dims = Airflow("gold_dims\n(mun→agr→vac→tempo)")
        g_not = Airflow("gold_fato_notificacao")
        g_obi = Airflow("gold_fato_obito")
        g_vac = Airflow("gold_fato_vacinacao")

    with Cluster(
        "Quality & Maint",
        graph_attr={"bgcolor": "#3b1f63", "fontcolor": "#f8fafc"},
    ):
        q_sil = Airflow("quality_gates_silver")
        q_gld = Airflow("quality_gates_gold")
        m_lgpd = Airflow("maint_lgpd_erasure")

    b_dengue >> s_not
    b_zika >> s_not
    b_chik >> s_not
    b_mun >> s_mun
    b_vac >> s_vac
    b_sim >> s_sim

    s_not >> g_not
    s_not >> g_dims
    s_mun >> g_dims
    s_vac >> g_vac
    s_vac >> g_dims
    s_sim >> g_obi

    s_not >> q_sil
    s_mun >> q_sil
    g_not >> q_gld
    g_dims >> q_gld
    g_vac >> q_gld
    g_obi >> q_gld
    g_not >> m_lgpd


print("✓ 05-fluxo-dags.png gerado")


# ── Diagrama 3: Stack Tecnológica ────────────────────────────────────────────

with Diagram(
    "Case Santander — Stack Tecnológica",
    filename=f"{OUTDIR}/06-stack-tecnologica",
    outformat="png",
    show=False,
    direction="TB",
    graph_attr={
        "bgcolor": "#0f172a",
        "fontcolor": "#f8fafc",
        "fontname": "Helvetica",
        "ranksep": "0.8",
        "nodesep": "0.5",
    },
    node_attr={"fontname": "Helvetica", "fontsize": "10"},
    edge_attr={"style": "invis"},
):
    with Cluster(
        "Ingestão & Orquestração",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#bfdbfe", "style": "rounded"},
    ):
        Airflow("Apache Airflow 2.9")
        Kafka("Kafka 7.6 (KRaft)")
        Pod("SparkOperator\n1.4.6 (k3s)")

    with Cluster(
        "Processamento",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#bbf7d0", "style": "rounded"},
    ):
        Spark("Apache Spark 3.5.1")
        MinIO("Delta Lake 3.1")
        MinIO("MinIO (s3a://)")

    with Cluster(
        "Armazenamento & Catálogo",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#fde68a", "style": "rounded"},
    ):
        PostgreSQL("PostgreSQL 16")
        Trino("Hive Metastore 4.0")
        MinIO("MinIO Object Store")

    with Cluster(
        "Serving & Análise",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#fecaca", "style": "rounded"},
    ):
        Trino("Trino 448")
        Metabase("Metabase v0.51")

    with Cluster(
        "Observabilidade",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#e9d5ff", "style": "rounded"},
    ):
        Prometheus("Prometheus 2.52")
        Grafana("Grafana 10.4")
        Marquez("Marquez 0.47\n(OpenLineage)")

    with Cluster(
        "CI/CD & Qualidade",
        graph_attr={"bgcolor": "#1e293b", "fontcolor": "#fed7aa", "style": "rounded"},
    ):
        from diagrams.onprem.ci import GithubActions

        GithubActions("GitHub Actions")
        from diagrams.programming.language import Python

        Python("ruff · bandit\npytest · GX")


print("✓ 06-stack-tecnologica.png gerado")
