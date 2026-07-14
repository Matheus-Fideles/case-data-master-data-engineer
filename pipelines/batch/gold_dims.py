"""Gold entry point: loads dimensions into the gold_dw schema (Postgres).

Thin dispatcher — selects the Strategy by name and delegates.
Adding a new dimension = creating a new file in gold/dims/ without touching this file.

Args:
  --dim           municipio | agravo | vacina | tempo
  --snapshot_date YYYYMMDD  (for dim_municipio)
  --ano_mes       YYYYMM    (for dim_vacina and dim_tempo)
  --batch_id      DAG run_id
"""

from __future__ import annotations

import argparse

from pipelines.batch.gold.base import DimLoader
from pipelines.batch.gold.dims.agravo import AgravoDimLoader
from pipelines.batch.gold.dims.municipio import MunicipioDimLoader
from pipelines.batch.gold.dims.tempo import TempoDimLoader
from pipelines.batch.gold.dims.vacina import VacinaDimLoader
from pipelines.common.postgres import make_pg_connection
from pipelines.common.spark import build_spark

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

    pg_url, pg_props = make_pg_connection()
    loader = _REGISTRY[args.dim](spark, pg_url, pg_props)
    count = loader.load(
        snapshot_date=args.snapshot_date,
        ano_mes=args.ano_mes,
        batch_id=args.batch_id,
    )
    print(f"[gold_dims/{args.dim}] {count} rows written")
    spark.stop()


if __name__ == "__main__":
    main()
