"""Gold entry point: loads facts into Delta Lake on MinIO.

Thin dispatcher — selects the Strategy by name and delegates.
Adding a new fact = creating a new file in the domain jobs/ without touching this file.

Args:
  --fato     notificacao | obito | vacinacao
  --ano_mes  YYYYMM  (notificacao, vacinacao)
  --ano      YYYY    (obito)
  --batch_id DAG run_id
"""

from __future__ import annotations

import argparse

from apps.epidemiologico.jobs.gold_fato_notificacao import NotificacaoFatoLoader
from apps.hospitalar.jobs.gold_fato_obito import ObitoFatoLoader
from apps.shared.gold_base import FatoLoader
from apps.shared.spark import build_spark
from apps.vacinal.jobs.gold_fato_vacinacao import VacinacaoFatoLoader

_REGISTRY: dict[str, type[FatoLoader]] = {
    "notificacao": NotificacaoFatoLoader,
    "obito": ObitoFatoLoader,
    "vacinacao": VacinacaoFatoLoader,
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

    loader = _REGISTRY[args.fato](spark)
    count = loader.load(
        ano_mes=args.ano_mes,
        ano=args.ano,
        batch_id=args.batch_id,
    )
    print(f"[gold_fatos/{args.fato}] {count} rows written")
    spark.stop()


if __name__ == "__main__":
    main()
