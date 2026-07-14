"""
Seed script: downloads samples from all APIs and saves to data/raw/.
Run via: make seed
Purpose: ensure OFFLINE_MODE=1 works in the demo without internet.

Uses a small page_size (100 records) — sufficient for demonstration.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("seed")

API_BASE = "https://apidadosabertos.saude.gov.br"
SEED_LIMIT = 100  # records per source — sufficient for demo
ANO = "2024"


def fetch_one_page(url: str, params: dict) -> list[dict]:
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # find the list inside the response
    for key, val in data.items():
        if isinstance(val, list):
            return val
    return []


def save(records: list[dict], fonte: str, data_ref: str) -> None:
    out_dir = Path("data/raw") / fonte / data_ref
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str, indent=2)
    log.info("  ✓ %s/%s — %d registros → %s", fonte, data_ref, len(records), out_file)


def seed_arbovirose(agravo: str, path: str) -> None:
    log.info("Fetching %s (%s)...", agravo, path)
    records = fetch_one_page(
        f"{API_BASE}{path}",
        {"nu_ano": ANO, "offset": 0, "limit": SEED_LIMIT},
    )
    assert records, f"API retornou vazio para {agravo}"
    save(records, agravo, ANO)


def seed_cnes() -> None:
    log.info("Fetching CNES...")
    records = fetch_one_page(
        f"{API_BASE}/cnes/estabelecimentos",
        {"offset": 0, "limit": SEED_LIMIT},
    )
    assert records, "API retornou vazio para CNES"
    save(records, "cnes", "20240101")


def seed_municipios() -> None:
    log.info("Fetching municipios...")
    resp = requests.get(
        f"{API_BASE}/macrorregiao-e-regiao-de-saude/municipio",
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    records = data.get("macrorregiao_regiao_saude_municipios", [])
    assert records, "API returned empty for municipios"
    save(records, "municipios", "20240101")


def seed_vacinacao() -> None:
    log.info("Fetching vacinacao PNI...")
    records = fetch_one_page(
        f"{API_BASE}/vacinacao/doses-aplicadas-pni-2024",
        {"page": 0, "size": SEED_LIMIT},
    )
    assert records, "API returned empty for vacinacao"
    save(records, "vacinacao_pni", ANO)


def seed_sim() -> None:
    log.info("Fetching SIM obitos...")
    records = fetch_one_page(
        f"{API_BASE}/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade",
        {"ano": ANO, "offset": 0, "limit": SEED_LIMIT},
    )
    assert records, "API retornou vazio para SIM"
    save(records, "sim", ANO)


def main() -> None:
    log.info("=== Seed data — downloading %d records per source ===", SEED_LIMIT)
    errors = []

    tasks = [
        ("dengue", lambda: seed_arbovirose("dengue", "/arboviroses/dengue")),
        ("zika", lambda: seed_arbovirose("zika", "/arboviroses/zikavirus")),
        ("chikungunya", lambda: seed_arbovirose("chikungunya", "/arboviroses/chikungunya")),
        ("cnes", seed_cnes),
        ("municipios", seed_municipios),
        ("vacinacao_pni", seed_vacinacao),
        ("sim", seed_sim),
    ]

    for name, fn in tasks:
        try:
            fn()
        except Exception as e:
            log.error("  ✗ %s — ERROR: %s", name, e)
            errors.append(name)

    if errors:
        log.error("Seed failed for: %s", ", ".join(errors))
        sys.exit(1)
    else:
        log.info("=== Seed complete. Use OFFLINE_MODE=1 for offline demo. ===")


if __name__ == "__main__":
    main()
