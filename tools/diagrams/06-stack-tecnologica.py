"""
Diagrama 6: Stack Tecnológica — visão por categoria (matplotlib).
Executa: python3 tools/diagrams/06-stack-tecnologica.py
Saída:   docs/assets/06-stack-tecnologica.png
"""
import os
BASE = os.path.join(os.path.dirname(__file__), "../..")
os.chdir(BASE)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

CATEGORIES = [
    {
        "title": "Ingestão &\nOrquestração",
        "color": "#E3F2FD",
        "border": "#1565C0",
        "items": ["Apache Airflow 2.9", "Kafka 3.7", "Kafka UI", "Extratores Python"],
    },
    {
        "title": "Processamento",
        "color": "#FFF3E0",
        "border": "#E65100",
        "items": ["Apache Spark 3.5", "Rancher k3s", "Spark Operator", "PySpark + Delta"],
    },
    {
        "title": "Armazenamento\n& Catálogo",
        "color": "#E8F5E9",
        "border": "#2E7D32",
        "items": ["MinIO (S3-compat)", "Delta Lake 3.1", "Hive Metastore 3.1", "PostgreSQL 15"],
    },
    {
        "title": "Serving\n& Análise",
        "color": "#F3E5F5",
        "border": "#6A1B9A",
        "items": ["Trino 448", "Metabase", "JDBC / REST API", "Star Schema"],
    },
    {
        "title": "Observabilidade",
        "color": "#FFF8E1",
        "border": "#F57F17",
        "items": ["Prometheus", "Grafana 10", "Marquez (Lineage)", "OpenLineage API"],
    },
    {
        "title": "CI/CD\n& Qualidade",
        "color": "#FCE4EC",
        "border": "#880E4F",
        "items": ["GitHub Actions", "Great Expectations", "pre-commit", "Ruff · mypy"],
    },
]

N = len(CATEGORIES)
FIG_W = 18
FIG_H = 5

fig, axes = plt.subplots(1, N, figsize=(FIG_W, FIG_H))
fig.patch.set_facecolor("white")

for ax, cat in zip(axes, CATEGORIES):
    ax.set_facecolor(cat["color"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Colored border
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_edgecolor(cat["border"])
        spine.set_linewidth(2.5)

    # Title
    ax.text(
        0.5, 0.92,
        cat["title"],
        ha="center", va="top",
        fontsize=11, fontweight="bold",
        color=cat["border"],
        transform=ax.transAxes,
    )

    # Items
    n_items = len(cat["items"])
    for i, item in enumerate(cat["items"]):
        y = 0.72 - i * (0.62 / max(n_items - 1, 1))
        # Pill background
        ax.add_patch(FancyBboxPatch(
            (0.08, y - 0.055), 0.84, 0.11,
            boxstyle="round,pad=0.01",
            facecolor="white",
            edgecolor=cat["border"],
            linewidth=1.2,
            transform=ax.transAxes,
            zorder=2,
        ))
        ax.text(
            0.5, y,
            item,
            ha="center", va="center",
            fontsize=9.5,
            color="#1A1A1A",
            transform=ax.transAxes,
            zorder=3,
        )

fig.suptitle(
    "Case Santander — Stack Tecnológica",
    fontsize=14, fontweight="bold", y=0.02, color="#333333",
)

plt.tight_layout(rect=[0, 0.04, 1, 1])
plt.savefig("docs/assets/06-stack-tecnologica.png", dpi=150, bbox_inches="tight", facecolor="white")
plt.close()
print("06-stack-tecnologica.png gerado")
