"""Aggregation and reproducible reporting.

A report run is fully described by four pins:

  * boundary_version   - which region geometry was used;
  * rule_version       - which boundary/overlap policies were used;
  * seq_from/seq_to    - the exact event batch (append-only seq window);
  * params_hash        - hash of the pins plus the boundary geometry hash and
                         rule content, so tampering with an old version is
                         detectable when reproducing a report.

Old reports stay reproducible because attributions for superseded versions are
never deleted, and the event stream is immutable. Comparing old vs new
boundaries for the *same* events is simply two aggregate queries over the same
seq window with different boundary_version pins.
"""
from __future__ import annotations

import hashlib
import json

from .storage import utcnow


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def aggregate(store, boundary_version: int, rule_version: int,
              seq_from: int = 1, seq_to: int | None = None) -> dict:
    """Aggregate attributions for one pinned (boundary, rule, event window)."""
    if seq_to is None:
        seq_to = store.max_seq()

    rows = store.conn.execute(
        """
        SELECT a.status, a.region_id, a.needs_review, COUNT(*) AS n
        FROM attributions a
        JOIN events e ON e.event_id = a.event_id
        WHERE a.boundary_version=? AND a.rule_version=?
          AND e.seq BETWEEN ? AND ?
        GROUP BY a.status, a.region_id, a.needs_review
        ORDER BY a.status, a.region_id
        """,
        (boundary_version, rule_version, seq_from, seq_to),
    ).fetchall()

    by_region: dict[str, dict] = {}
    by_status: dict[str, int] = {}
    total = 0
    review = 0
    for row in rows:
        n = row["n"]
        total += n
        by_status[row["status"]] = by_status.get(row["status"], 0) + n
        if row["needs_review"]:
            review += n
        key = row["region_id"] or "__UNLOCATED__"
        bucket = by_region.setdefault(
            key, {"region_id": row["region_id"], "total": 0,
                  "by_status": {}}
        )
        bucket["total"] += n
        bucket["by_status"][row["status"]] = (
            bucket["by_status"].get(row["status"], 0) + n
        )

    window_count = store.conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE seq BETWEEN ? AND ?",
        (seq_from, seq_to),
    ).fetchone()["n"]

    return {
        "boundary_version": boundary_version,
        "rule_version": rule_version,
        "seq_from": seq_from,
        "seq_to": seq_to,
        "events_in_window": window_count,
        "events_attributed_in_window": total,
        "events_pending_attribution": window_count - total,
        "by_status": dict(sorted(by_status.items())),
        "needs_review": review,
        "by_region": [by_region[k] for k in sorted(by_region)],
    }


def compare(store, old_version: int, new_version: int, rule_version: int,
            seq_from: int = 1, seq_to: int | None = None) -> dict:
    """Per-event, per-region comparison of two boundary versions.

    Both aggregations use the identical event window (same batch of events);
    only the boundary pin differs.
    """
    if seq_to is None:
        seq_to = store.max_seq()
    old_rows = store.conn.execute(
        """
        SELECT e.event_id, e.lon, e.lat, a.status, a.region_id,
               a.needs_review
        FROM events e JOIN attributions a ON a.event_id=e.event_id
        WHERE a.boundary_version=? AND a.rule_version=?
          AND e.seq BETWEEN ? AND ?
        """,
        (old_version, rule_version, seq_from, seq_to),
    ).fetchall()
    new_rows = {
        r["event_id"]: r
        for r in store.conn.execute(
            """
            SELECT e.event_id, a.status, a.region_id, a.needs_review
            FROM events e JOIN attributions a ON a.event_id=e.event_id
            WHERE a.boundary_version=? AND a.rule_version=?
              AND e.seq BETWEEN ? AND ?
            """,
            (new_version, rule_version, seq_from, seq_to),
        )
    }

    changed, same, only_old, only_new = [], [], [], []
    region_flow: dict[str, dict] = {}
    for row in old_rows:
        other = new_rows.get(row["event_id"])
        if other is None:
            only_old.append(row["event_id"])
            continue
        old_key = row["region_id"] or f"UNLOCATED({row['status']})"
        new_key = other["region_id"] or f"UNLOCATED({other['status']})"
        if old_key == new_key:
            same.append(row["event_id"])
        else:
            changed.append({
                "event_id": row["event_id"],
                "lon": row["lon"],
                "lat": row["lat"],
                "old_region": row["region_id"],
                "old_status": row["status"],
                "new_region": other["region_id"],
                "new_status": other["status"],
            })
            flow = region_flow.setdefault(
                old_key, {"from": old_key, "to": {}, "count": 0}
            )
            flow["to"][new_key] = flow["to"].get(new_key, 0) + 1
            flow["count"] += 1
    old_ids = {r["event_id"] for r in old_rows}
    for eid, row in new_rows.items():
        if eid not in old_ids:
            only_new.append(eid)

    flows = sorted(
        (
            {"from": v["from"],
             "to": [{"region": k, "count": n} for k, n in
                    sorted(v["to"].items())],
             "count": v["count"]}
            for v in region_flow.values()
        ),
        key=lambda f: (-f["count"], f["from"]),
    )
    return {
        "seq_from": seq_from,
        "seq_to": seq_to,
        "old_boundary_version": old_version,
        "new_boundary_version": new_version,
        "rule_version": rule_version,
        "events_unchanged": len(same),
        "events_changed": len(changed),
        "only_in_old_version": only_old,
        "only_in_new_version": only_new,
        "region_flows": flows,
        "changed_event_samples": changed[:200],
    }


def _params_hash(store, boundary_version, rule_version, seq_from, seq_to):
    bs = store.get_boundary_set(boundary_version)
    rule = store.get_rule(rule_version)
    if bs is None or rule is None:
        raise ValueError("unknown boundary/rule version for report")
    payload = json.dumps({
        "boundary_version": boundary_version,
        "boundary_geometry_hash": bs["geometry_hash"],
        "rule_version": rule_version,
        "rule_on_boundary": rule["on_boundary"],
        "rule_overlap": rule["overlap"],
        "rule_unlocated": rule["unlocated"],
        "seq_from": seq_from,
        "seq_to": seq_to,
    }, sort_keys=True)
    return _sha256(payload)


def run_report(store, boundary_version: int, rule_version: int | None = None,
               seq_from: int = 1, seq_to: int | None = None,
               compare_version: int | None = None) -> dict:
    """Compute, persist and return a reproducible report (optionally compared)."""
    if rule_version is None:
        rule_version = store.get_active_versions()["rule_version"]
    if seq_to is None:
        seq_to = store.max_seq()
    params_hash = _params_hash(
        store, boundary_version, rule_version, seq_from, seq_to
    )
    totals = aggregate(store, boundary_version, rule_version, seq_from, seq_to)
    comparison = None
    if compare_version is not None:
        comparison = compare(
            store, compare_version, boundary_version, rule_version,
            seq_from, seq_to,
        )
    report_id = store.insert_report(
        boundary_version, rule_version, compare_version, seq_from, seq_to,
        params_hash, totals, comparison,
    )
    return {
        "report_id": report_id,
        "created_at": utcnow(),
        "params_hash": params_hash,
        "totals": totals,
        "comparison": comparison,
    }


def reproduce_report(store, report_id: int) -> dict:
    """Re-run a stored report with its original pins; hashes must match."""
    row = store.conn.execute(
        "SELECT * FROM report_runs WHERE report_id=?", (report_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"unknown report {report_id}")
    fresh = run_report(
        store,
        boundary_version=row["boundary_version"],
        rule_version=row["rule_version"],
        seq_from=row["seq_from"],
        seq_to=row["seq_to"],
        compare_version=row["compare_version"],
    )
    if fresh["params_hash"] != row["params_hash"]:
        raise RuntimeError(
            f"reproduction of report {report_id} produced a different params "
            f"hash: stored {row['params_hash'][:12]} vs "
            f"fresh {fresh['params_hash'][:12]} - underlying version data changed"
        )
    return {
        "report_id": report_id,
        "reproduced": fresh,
        "stored_params_hash": row["params_hash"],
        "hash_matches": True,
        "stored_created_at": row["created_at"],
    }
