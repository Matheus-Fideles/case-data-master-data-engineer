"""Bootstrap Metabase dashboards for epidemiological surveillance.

Creates:
  - Trino data source pointing at gold_dw via jdbc
  - "Epidemiological Overview" dashboard with 4 cards:
      1. Notifications by disease type (bar chart)
      2. Vaccination coverage by municipality (table)
      3. Deaths by municipality (bar chart)
      4. SCD2 patient dimension status (scalar — current versions)

Usage:
  python infra/metabase/setup_dashboards.py [--host HOST] [--port PORT]

Requires Metabase to be running and accessible.
Idempotent: skips creation if the database or dashboard already exists.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

import requests

log = logging.getLogger(__name__)

_DEFAULT_HOST = "http://localhost"
_DEFAULT_PORT = 3001

# ---------------------------------------------------------------------------
# Metabase API helpers
# ---------------------------------------------------------------------------

class MetabaseClient:
    def __init__(self, base_url: str, session_token: str) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": session_token,
        }

    def get(self, path: str) -> dict | list:
        resp = requests.get(f"{self._base}{path}", headers=self._headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, payload: dict) -> dict:
        resp = requests.post(
            f"{self._base}{path}", json=payload, headers=self._headers, timeout=15
        )
        resp.raise_for_status()
        return resp.json()

    def put(self, path: str, payload: dict) -> dict:
        resp = requests.put(
            f"{self._base}{path}", json=payload, headers=self._headers, timeout=15
        )
        resp.raise_for_status()
        return resp.json()


def authenticate(base_url: str, username: str, password: str) -> MetabaseClient:
    resp = requests.post(
        f"{base_url}/api/session",
        json={"username": username, "password": password},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json()["id"]
    log.info("Authenticated to Metabase as %s", username)
    return MetabaseClient(base_url, token)


# ---------------------------------------------------------------------------
# Data source setup
# ---------------------------------------------------------------------------

def ensure_trino_database(client: MetabaseClient, trino_host: str = "trino", trino_port: int = 8080) -> int:
    """Creates the Trino database connector in Metabase if it doesn't exist."""
    databases = client.get("/api/database")
    for db in databases.get("data", databases if isinstance(databases, list) else []):
        if db.get("name") == "Gold DW (Trino)":
            log.info("Trino database already exists (id=%d)", db["id"])
            return db["id"]

    db = client.post("/api/database", {
        "name": "Gold DW (Trino)",
        "engine": "presto",
        "details": {
            "host": trino_host,
            "port": trino_port,
            "catalog": "postgresql",
            "schema": "gold_dw",
            "user": "trino",
            "ssl": False,
        },
        "auto_run_queries": True,
    })
    log.info("Created Trino database (id=%d)", db["id"])
    return db["id"]


# ---------------------------------------------------------------------------
# Question (card) builders
# ---------------------------------------------------------------------------

def _native_card(name: str, sql: str, db_id: int, display: str = "bar") -> dict:
    return {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": {},
    }


_CARDS = [
    {
        "name": "Notifications by Disease Type",
        "sql": """
SELECT d.nome_agravo AS disease, COUNT(*) AS notifications
FROM gold_dw.fato_notificacao f
JOIN gold_dw.dim_agravo d ON f.sk_agravo = d.sk_agravo
GROUP BY d.nome_agravo
ORDER BY notifications DESC
LIMIT 20
""".strip(),
        "display": "bar",
    },
    {
        "name": "Vaccination Coverage by Municipality",
        "sql": """
SELECT m.nome_municipio AS municipality, m.uf AS state,
       COUNT(*) AS doses_applied
FROM gold_dw.fato_vacinacao f
JOIN gold_dw.dim_municipio m ON f.sk_municipio_aplicacao = m.sk_municipio
GROUP BY m.nome_municipio, m.uf
ORDER BY doses_applied DESC
LIMIT 50
""".strip(),
        "display": "table",
    },
    {
        "name": "Deaths by Municipality",
        "sql": """
SELECT m.nome_municipio AS municipality, m.uf AS state,
       COUNT(*) AS deaths
FROM gold_dw.fato_obito f
JOIN gold_dw.dim_municipio m ON f.sk_municipio_residencia = m.sk_municipio
GROUP BY m.nome_municipio, m.uf
ORDER BY deaths DESC
LIMIT 20
""".strip(),
        "display": "bar",
    },
    {
        "name": "Active Patient Records (SCD2)",
        "sql": """
SELECT COUNT(*) AS current_patient_versions
FROM gold_dw.dim_paciente
WHERE is_current = TRUE
""".strip(),
        "display": "scalar",
    },
]


def ensure_cards(client: MetabaseClient, db_id: int) -> list[int]:
    """Creates question cards and returns their IDs."""
    existing = {c["name"]: c["id"] for c in client.get("/api/card")}
    card_ids = []
    for spec in _CARDS:
        if spec["name"] in existing:
            log.info("Card already exists: %s (id=%d)", spec["name"], existing[spec["name"]])
            card_ids.append(existing[spec["name"]])
            continue

        card = client.post("/api/card", _native_card(spec["name"], spec["sql"], db_id, spec["display"]))
        log.info("Created card: %s (id=%d)", spec["name"], card["id"])
        card_ids.append(card["id"])

    return card_ids


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def ensure_dashboard(client: MetabaseClient, card_ids: list[int]) -> int:
    """Creates the Epidemiological Overview dashboard if it doesn't exist."""
    dashboards = client.get("/api/dashboard")
    for d in dashboards:
        if d.get("name") == "Epidemiological Overview":
            log.info("Dashboard already exists (id=%d)", d["id"])
            return d["id"]

    dashboard = client.post("/api/dashboard", {
        "name": "Epidemiological Overview",
        "description": "Epidemiological surveillance KPIs — notifications, vaccination, deaths.",
    })
    dash_id = dashboard["id"]

    # Add cards in a 2×2 grid layout
    positions = [
        (0, 0), (0, 12),
        (8, 0), (8, 12),
    ]
    for card_id, (row, col) in zip(card_ids, positions):
        client.post(f"/api/dashboard/{dash_id}/cards", {
            "cardId": card_id,
            "row": row,
            "col": col,
            "sizeX": 12,
            "sizeY": 8,
        })

    log.info("Created dashboard 'Epidemiological Overview' (id=%d)", dash_id)
    return dash_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def wait_for_metabase(base_url: str, timeout: int = 120) -> None:
    log.info("Waiting for Metabase at %s ...", base_url)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(f"{base_url}/api/health", timeout=5)
            if resp.ok:
                log.info("Metabase is up.")
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError(f"Metabase did not become healthy within {timeout}s")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Bootstrap Metabase dashboards")
    parser.add_argument("--host", default=_DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT)
    parser.add_argument("--user", default="admin@local.dev")
    parser.add_argument("--password", default="admin123")
    parser.add_argument("--trino-host", default="trino")
    parser.add_argument("--trino-port", type=int, default=8080)
    args = parser.parse_args()

    base_url = f"{args.host}:{args.port}"

    wait_for_metabase(base_url)
    client = authenticate(base_url, args.user, args.password)

    db_id = ensure_trino_database(client, args.trino_host, args.trino_port)
    card_ids = ensure_cards(client, db_id)
    dash_id = ensure_dashboard(client, card_ids)

    print(f"\nDashboard ready: {base_url}/dashboard/{dash_id}")
    print("  Notifications by Disease Type")
    print("  Vaccination Coverage by Municipality")
    print("  Deaths by Municipality")
    print("  Active Patient Records (SCD2)")


if __name__ == "__main__":
    main()
