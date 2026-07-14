"""Gold entry point: loads facts into the gold_dw schema (Postgres).

Thin dispatcher — selects the Strategy by name and delegates.
Adding a new fact = creating a new file in gold/fatos/ without touching this file.

Args:
  --fato     notificacao | obito | vacinacao
  --ano_mes  YYYYMM  (notificacao, vacinacao)
  --ano      YYYY    (obito)
  --batch_id DAG run_id
"""
from __future__ import annotations

import argparse

from pipelines.batch.gold.base import FatoLoader
from pipelines.batch.gold.fatos.notificacao import NotificacaoFatoLoader
from pipelines.batch.gold.fatos.obito import ObitoFatoLoader
from pipelines.batch.gold.fatos.vacinacao import VacinacaoFatoLoader
from pipelines.common.postgres import make_pg_connection
from pipelines.common.spark import build_spark

_REGISTRY: dict[str, type[FatoLoader]] = {
    "notificacao": NotificacaoFatoLoader,
    "obito":       ObitoFatoLoader,
    "vacinacao":   VacinacaoFatoLoader,
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--fato", required=True, choices=list(_REGISTRY))
    p.add_argument("--ano_mes", default=None)
    p.add_argument("--ano", default=None)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("gold_fatos")
    spark.sparkContext.setLogLevel("WARN")

    pg_url, pg_props = make_pg_connection()
    loader = _REGISTRY[args.fato](spark, pg_url, pg_props)
    count = loader.load(
        ano_mes=args.ano_mes,
        ano=args.ano,
        batch_id=args.batch_id,
    )
    print(f"[gold_fatos/{args.fato}] {count} rows written")
    spark.stop()


if __name__ == "__main__":
    main()
