"""Application services: the use-case orchestration shared by CLI and web.

State machine
-------------
  upload boundary -> DRAFT boundary_set + impact analysis -> recompute job
  (PENDING/RUNNING/INTERRUPTED/COMPLETED) -> only then ACTIVE; the previously
  active set becomes SUPERSEDED but all of its attribution rows are retained.

  propose rule -> PENDING -> approve -> active rule version used by both live
  attribution and any subsequent job.

The web layer and CLI are thin wrappers around these functions, so the same
validation and the same determinism guarantees apply regardless of entrypoint.
"""
from __future__ import annotations

import json

from .boundaries import (
    analyze_impact,
    parse_boundary_geojson,
    persist_boundary_set,
    recommend_scope,
)
from .storage import Store, utcnow
from .worker import Worker, validate_rule_policies

DEFAULT_RULE = {
    "on_boundary": "MIN_ID",
    "overlap": "MIN_ID",
    "unlocated": "UNLOCATED",
    "rationale": (
        "seed rule: deterministic smallest-region-id tie break; "
        "no random assignment; UNLOCATED stays unassigned"
    ),
}


def ensure_defaults(store: Store) -> dict:
    """Create the seed rule on first use. Idempotent."""
    active_rule = store.get_active_versions()["rule_version"]
    if active_rule is not None:
        return {"rule_version": active_rule, "created": False}
    rv = store.insert_rule(
        DEFAULT_RULE["on_boundary"], DEFAULT_RULE["overlap"],
        rationale=DEFAULT_RULE["rationale"], state="APPROVED",
    )
    store.set_meta("active_rule_version", str(rv))
    return {"rule_version": rv, "created": True}


def upload_boundary(store: Store, geojson_doc: dict,
                    source_name: str | None = None) -> dict:
    """Validate + persist an uploaded boundary set as a new DRAFT version."""
    ensure_defaults(store)
    parsed = parse_boundary_geojson(geojson_doc, source_name)
    with store.tx():
        version = persist_boundary_set(store, parsed, source_name)
    return {
        "boundary_version": version,
        "state": "DRAFT",
        "geometry_hash": parsed["hash"],
        "region_count": len(parsed["regions"]),
        "self_intersects": parsed["self_intersects"],
        "validation_notes": parsed["notes"],
    }


def create_recompute_job(store: Store, new_version: int,
                         scope: str | None = None,
                         rule_version: int | None = None) -> dict:
    """Create a recompute job with impact evidence pinned into the row."""
    bs = store.get_boundary_set(new_version)
    if bs is None:
        raise ValueError(f"unknown boundary version {new_version}")
    if bs["state"] not in ("DRAFT",):
        raise ValueError(
            f"boundary version {new_version} is {bs['state']}, not DRAFT"
        )
    active = store.get_active_versions()
    old_version = active["boundary_version"]
    if rule_version is None:
        rule_version = active["rule_version"]
    rule = store.get_rule(rule_version)
    if rule is None or rule["state"] != "APPROVED":
        raise ValueError(f"rule {rule_version} is not an APPROVED rule")

    impact = analyze_impact(store, old_version, new_version)
    if scope is None:
        scope = recommend_scope(impact)
    if scope not in ("FULL", "CHANGED_REGIONS"):
        raise ValueError(f"invalid scope {scope!r}")
    if scope == "FULL":
        # Full recompute touches every event up to the snapshot; record that
        # explicitly so the job row documents its own blast radius.
        impact = dict(impact)
        impact["affected_event_ids"] = [
            r["event_id"] for r in store.list_events()
        ]
        impact["affected_event_count"] = len(impact["affected_event_ids"])
        impact["scope_override"] = "FULL requested explicitly"

    # Pinned snapshot: events arriving later are invisible to this job.
    snapshot_seq = store.max_seq()
    job_id = store.create_job(
        old_version, new_version, rule_version, scope, impact, snapshot_seq
    )
    return {
        "job_id": job_id,
        "boundary_version_from": old_version,
        "boundary_version_to": new_version,
        "rule_version": rule_version,
        "scope": scope,
        "snapshot_seq": snapshot_seq,
        "state": "PENDING",
        "impact": impact,
    }


def activate_version(store: Store, version: int) -> dict:
    """Promote a DRAFT to ACTIVE; refused while its job is unfinished."""
    ok = store.activate_boundary_set(version)
    if not ok:
        raise RuntimeError(
            f"cannot activate version {version}: missing, already active, or a "
            "recompute job for it is still PENDING/RUNNING/INTERRUPTED"
        )
    # Backfill: any event that arrived while the new version was a draft now
    # needs an attribution row under the newly active version.
    worker = Worker(store)
    backfill = worker.attribute_pending_live_events()
    return {"boundary_version": version, "state": "ACTIVE", "backfill": backfill}


def propose_rule(store: Store, on_boundary: str, overlap: str,
                 rationale: str | None = None) -> dict:
    validate_rule_policies(on_boundary, overlap)
    rv = store.insert_rule(on_boundary, overlap, rationale=rationale)
    return {"rule_version": rv, "state": "PENDING",
            "on_boundary": on_boundary, "overlap": overlap}


def approve_rule(store: Store, rule_version: int) -> dict:
    if not store.approve_rule(rule_version):
        raise RuntimeError(
            f"rule {rule_version} cannot be approved (missing or not PENDING)"
        )
    return {"rule_version": rule_version, "state": "APPROVED"}


def resolve_conflict(store: Store, conflict_id: int, chosen_region_id: str,
                     decided_by: str = "operator") -> dict:
    ok = store.resolve_conflict(conflict_id, chosen_region_id, decided_by)
    if not ok:
        raise RuntimeError(f"conflict {conflict_id} is not OPEN")
    return {"conflict_id": conflict_id, "state": "RESOLVED",
            "chosen_region_id": chosen_region_id}


def interrupt_job(store: Store, job_id: int) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise ValueError(f"unknown job {job_id}")
    if job["state"] not in ("RUNNING", "PENDING"):
        raise RuntimeError(f"job {job_id} is {job['state']}, cannot interrupt")
    Worker.request_interrupt(store, job_id)
    return {"job_id": job_id, "interrupt_requested": True, "at": utcnow()}


def system_status(store: Store) -> dict:
    active = store.get_active_versions()
    return {
        "active_versions": active,
        "total_events": store.max_seq(),
        "boundary_sets": [dict(b) for b in store.list_boundary_sets()],
        "rules": [dict(r) for r in store.list_rules()],
        "jobs": [dict(j) for j in store.list_jobs()],
        "open_conflicts": [
            dict(c) for c in store.list_conflicts(state="OPEN")
        ],
        "unlocated_counts": _unlocated_counts(store, active),
    }


def _unlocated_counts(store: Store, active: dict) -> dict:
    bv = active["boundary_version"]
    rv = active["rule_version"]
    if bv is None or rv is None:
        return {}
    rows = store.conn.execute(
        "SELECT status, COUNT(*) n FROM attributions "
        "WHERE boundary_version=? AND rule_version=? GROUP BY status",
        (bv, rv),
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def ingest_event(store: Store, event_id: str, lon: float, lat: float,
                 payload=None) -> dict:
    """Validate and append one event, then attribute it under active versions."""
    from .geometry import GeometryError, _as_point

    x, y = _as_point((lon, lat))
    if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
        raise GeometryError(
            f"lon/lat ({x},{y}) outside EPSG:4326 ranges "
            "lon [-180,180], lat [-90,90]"
        )
    seq = store.insert_event(event_id, x, y, payload)
    worker = Worker(store)
    info = worker.attribute_pending_live_events()
    return {"event_id": event_id, "seq": seq, "attribution": info}
