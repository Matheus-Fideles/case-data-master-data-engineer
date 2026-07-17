"""Gold entry point: loads dimensions into Delta Lake on MinIO.

Thin dispatcher — selects the Strategy by name and delegates.
Adding a new dimension = creating a new file in the domain jobs/ without touching this file.

Args:
  --dim           municipio | agravo | vacina | tempo
  --snapshot_date YYYYMMDD  (for dim_municipio)
  --ano_mes       YYYYMM    (for dim_vacina and dim_tempo)
  --batch_id      DAG run_id
"""

from __future__ import annotations

import argparse

from apps.epidemiologico.jobs.gold_dim_agravo import AgravoDimLoader
from apps.epidemiologico.jobs.gold_dim_tempo import TempoDimLoader
from apps.geografico.jobs.gold_dim_municipio import MunicipioDimLoader
from apps.shared.gold_base import DimLoader
from apps.shared.spark import build_spark
from apps.vacinal.jobs.gold_dim_vacina import VacinaDimLoader

_REGISTRY: dict[str, type[DimLoader]] = {
    "municipio": MunicipioDimLoader,
    "agravo": AgravoDimLoader,
    "vacina": VacinaDimLoader,
    "tempo": TempoDimLoader,
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dim", required=True, choices=list(_REGISTRY))
    p.add_argument("--snapshot_date", default=None)
    p.add_argument("--ano_mes", default=None)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("gold_dims")
    spark.sparkContext.setLogLevel("WARN")

    loader = _REGISTRY[args.dim](spark)
    count = loader.load(
        snapshot_date=args.snapshot_date,
        ano_mes=args.ano_mes,
        batch_id=args.batch_id,
    )
    print(f"[gold_dims/{args.dim}] {count} rows written")
    spark.stop()


if __name__ == "__main__":
    main()
