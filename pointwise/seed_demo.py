"""Deterministic demo dataset: adjacent regions, a shared edge, an overlap.

Used by `python -m pointwise demo` and by the test-suite smoke test. All
coordinates are hand-picked (no randomness) so demo output is reproducible.
"""
from __future__ import annotations

from . import app as services
from .worker import Worker

# v1: A (west) and B (east) share the edge x=116.50, y in [39.8, 40.2].
#     C overlaps the north-east corner of B (deliberate footprint overlap).
BOUNDARY_V1 = {
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature",
         "properties": {"region_id": "A", "name": "甲区"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.0, 39.8], [116.5, 39.8], [116.5, 40.2],
             [116.0, 40.2], [116.0, 39.8]]]}},
        {"type": "Feature",
         "properties": {"region_id": "B", "name": "乙区"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.5, 39.8], [117.0, 39.8], [117.0, 40.2],
             [116.5, 40.2], [116.5, 39.8]]]}},
        {"type": "Feature",
         "properties": {"region_id": "C", "name": "丙区（与乙区重叠）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.8, 40.05], [117.0, 40.05], [117.0, 40.2],
             [116.8, 40.2], [116.8, 40.05]]]}},
        {"type": "Feature",
         "properties": {"region_id": "E", "name": "戊区（远离改动带，两版不变）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [120.0, 30.0], [121.0, 30.0], [121.0, 31.0],
             [120.0, 31.0], [120.0, 30.0]]]}},
    ],
}

# v2: the shared edge moves east to x=116.6 (one event changes ownership),
#     B's southern edge moves north (another event becomes UNLOCATED), and a
#     new region D appears. C is unchanged.
BOUNDARY_V2 = {
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature",
         "properties": {"region_id": "A", "name": "甲区（东扩）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.0, 39.8], [116.6, 39.8], [116.6, 40.2],
             [116.0, 40.2], [116.0, 39.8]]]}},
        {"type": "Feature",
         "properties": {"region_id": "B", "name": "乙区（南界北移）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.6, 39.9], [117.0, 39.9], [117.0, 40.2],
             [116.6, 40.2], [116.6, 39.9]]]}},
        {"type": "Feature",
         "properties": {"region_id": "C", "name": "丙区（不变）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [116.8, 40.05], [117.0, 40.05], [117.0, 40.2],
             [116.8, 40.2], [116.8, 40.05]]]}},
        {"type": "Feature",
         "properties": {"region_id": "D", "name": "丁区（新增）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [117.1, 39.8], [117.4, 39.8], [117.4, 40.2],
             [117.1, 40.2], [117.1, 39.8]]]}},
        {"type": "Feature",
         "properties": {"region_id": "E", "name": "戊区（几何与位置均不变）"},
         "geometry": {"type": "Polygon", "coordinates": [[
             [120.0, 30.0], [121.0, 30.0], [121.0, 31.0],
             [120.0, 31.0], [120.0, 30.0]]]}},
    ],
}

EVENTS = [
    # event_id, lon, lat, expected v1 outcome under MIN_ID rule
    ("e-001", 116.20, 40.00, "inside A"),
    ("e-002", 116.80, 39.90, "inside B"),
    ("e-003", 116.50, 40.00, "on A/B shared edge -> ON_BOUNDARY, MIN_ID=A"),
    ("e-004", 116.90, 40.12, "inside B and C overlap -> OVERLAP, MIN_ID=B"),
    ("e-005", 117.30, 40.00, "outside every region v1 (lands in new D in v2)"),
    ("e-006", 116.55, 40.00, "inside B v1; inside A v2 -> ownership change"),
    ("e-007", 116.80, 39.85, "inside B v1; UNLOCATED v2 (south edge moved)"),
    ("e-008", 116.30, 39.90, "inside changed region A's bbox -> candidate"),
    ("e-009", 120.50, 30.50, "inside unchanged region E; never affected"),
]


def seed_demo(store) -> dict:
    services.ensure_defaults(store)
    v1 = services.upload_boundary(store, BOUNDARY_V1, "demo-v1")
    services.activate_version(store, v1["boundary_version"])

    ingested = []
    for eid, lon, lat, _note in EVENTS:
        ingested.append(services.ingest_event(store, eid, lon, lat)["event_id"])

    # A stricter, human-in-the-loop rule, left PENDING for the web demo flow.
    pending_rule = services.propose_rule(
        store, "MANUAL", "MANUAL",
        rationale="demo: edge/overlap points must be reviewed by an operator",
    )

    v2 = services.upload_boundary(store, BOUNDARY_V2, "demo-v2")
    job = services.create_recompute_job(
        store, v2["boundary_version"]
    )  # scope chosen from impact evidence
    Worker(store).run_job(job["job_id"])
    job_row = store.get_job(job["job_id"])

    from .reports import run_report

    report_v1 = run_report(store, v1["boundary_version"])
    report_cmp = run_report(
        store, v2["boundary_version"],
        compare_version=v1["boundary_version"],
    )
    return {
        "boundary_v1": v1["boundary_version"],
        "boundary_v2_draft": v2["boundary_version"],
        "events": ingested,
        "pending_rule_version": pending_rule["rule_version"],
        "job": {"id": job["job_id"], "scope": job["scope"],
                "state": job_row["state"],
                "impact": job["impact"]},
        "report_v1_id": report_v1["report_id"],
        "comparison_report_id": report_cmp["report_id"],
        "next_steps": [
            "open the web console to review ON_BOUNDARY / OVERLAP conflicts",
            "approve rule v2 (MANUAL) or keep the seed MIN_ID rule",
            "activate boundary v2 only after its COMPLETED recompute job",
            "reproduce any report via /api/reports/<id>/reproduce",
        ],
    }
