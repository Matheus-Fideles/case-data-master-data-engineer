"""OpenLineage client for explicit lineage emission in non-Spark contexts.

Used by:
  - Extraction PythonOperators (landing → Marquez)
  - emit_lineage tasks in Airflow DAGs
  - Maintenance utilities

For Spark jobs (Bronze/Silver/Gold), the automatic listener
`io.openlineage.spark.agent.OpenLineageSparkListener` captures everything via
`spark.extraListeners` — calling this module is not necessary for those jobs.

Expected env vars:
  OPENLINEAGE_URL        Base URL of Marquez (e.g.: http://marquez:5000)
  OPENLINEAGE_NAMESPACE  Namespace for grouping (e.g.: "batch", "streaming")

If OPENLINEAGE_URL is not set, all functions are no-ops.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

log = logging.getLogger(__name__)

_OL_URL       = os.environ.get("OPENLINEAGE_URL", "").rstrip("/")
_OL_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "batch")
_PRODUCER     = "https://github.com/matheusfideles/case-data-master-data-engineer"


@dataclass
class Dataset:
    """Represents an input or output dataset in the lineage graph."""
    name: str                           # e.g.: "s3://bronze/dengue/"
    namespace: str = "s3"              # e.g.: "s3", "postgres", "kafka"
    facets: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def s3(cls, path: str, **facets) -> "Dataset":
        return cls(name=path, namespace="s3", facets=facets)

    @classmethod
    def postgres(cls, schema: str, table: str, **facets) -> "Dataset":
        host = os.environ.get("POSTGRES_HOST", "postgres")
        return cls(
            name=f"{schema}.{table}",
            namespace=f"postgresql://{host}:5432",
            facets=facets,
        )

    @classmethod
    def kafka(cls, topic: str, **facets) -> "Dataset":
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        return cls(name=topic, namespace=f"kafka://{bootstrap}", facets=facets)


def emit_start(
    job_name: str,
    run_id: str | None = None,
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
    job_facets: dict[str, Any] | None = None,
) -> str | None:
    """Emits a START lineage event. Returns the generated run_id (or None if disabled)."""
    if not _OL_URL:
        return None
    rid = run_id or str(uuid4())
    _post_event("START", job_name, rid, inputs or [], outputs or [], job_facets or {})
    return rid


def emit_complete(
    job_name: str,
    run_id: str,
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
    output_facets: dict[str, Any] | None = None,
) -> None:
    """Emits a COMPLETE lineage event."""
    if not _OL_URL:
        return
    _post_event("COMPLETE", job_name, run_id, inputs or [], outputs or [], {}, output_facets)


def emit_fail(
    job_name: str,
    run_id: str,
    error: str = "",
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
) -> None:
    """Emits a FAIL lineage event."""
    if not _OL_URL:
        return
    facets = {}
    if error:
        facets["errorMessage"] = {
            "_producer": _PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": error[:500],
        }
    _post_event("FAIL", job_name, run_id, inputs or [], outputs or [], {}, None, facets)


# ── Decorator helper ──────────────────────────────────────────────────────────

def with_lineage(
    job_name: str,
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
):
    """Decorator that wraps a function with automatic START/COMPLETE/FAIL emission."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            run_id = emit_start(job_name, inputs=inputs, outputs=outputs)
            try:
                result = fn(*args, **kwargs)
                emit_complete(job_name, run_id or "", inputs=inputs, outputs=outputs)
                return result
            except Exception as exc:
                emit_fail(job_name, run_id or "", error=str(exc), inputs=inputs, outputs=outputs)
                raise
        wrapper.__name__ = fn.__name__
        return wrapper
    return decorator


# ── Internal implementation ───────────────────────────────────────────────────

def _post_event(
    event_type: str,
    job_name: str,
    run_id: str,
    inputs: list[Dataset],
    outputs: list[Dataset],
    job_facets: dict,
    output_dataset_facets: dict | None = None,
    run_facets: dict | None = None,
) -> None:
    try:
        import requests

        now = datetime.now(tz=timezone.utc).isoformat()

        def _ds(ds: Dataset) -> dict:
            d: dict = {
                "namespace": ds.namespace,
                "name": ds.name,
            }
            if ds.facets:
                d["facets"] = ds.facets
            return d

        payload = {
            "eventType": event_type,
            "eventTime": now,
            "run": {
                "runId": run_id,
                "facets": run_facets or {},
            },
            "job": {
                "namespace": _OL_NAMESPACE,
                "name": job_name,
                "facets": {
                    "documentation": {
                        "_producer": _PRODUCER,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
                        "description": f"Data Lake pipeline: {job_name}",
                    },
                    **job_facets,
                },
            },
            "inputs": [_ds(ds) for ds in inputs],
            "outputs": [_ds(ds) for ds in outputs],
            "producer": _PRODUCER,
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        }

        resp = requests.post(
            f"{_OL_URL}/api/v1/lineage",
            json=payload,
            timeout=5,
        )
        if resp.status_code not in (200, 201, 204):
            log.warning(
                "[lineage] Marquez returned %d for event %s/%s",
                resp.status_code, event_type, job_name,
            )
    except Exception as e:
        log.warning("[lineage] Failed to emit event %s: %s", event_type, e)
