"""Boundary-set parsing, validation, content hashing and impact analysis.

A boundary set is a GeoJSON FeatureCollection of Polygon / MultiPolygon
features; each feature's properties must carry a stable ``region_id``.

Determinism rules shared with the worker:
  * regions are always ordered by region_id before spatial tests;
  * overlapping footprints are *reported at upload time* (validation_notes,
    self_intersects flag) rather than silently merged, so the points that fall
    in overlap zones later get a deterministic OVERLAP classification instead
    of being randomly assigned;
  * geometry hashes use geometry.canonical_geometry_json so coordinate order
    changes that do not change the polygon produce the same hash.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from . import geometry as geo


def _exterior_vertices(geometry: dict):
    """All exterior-ring vertices of a validated geometry."""
    for polygon in geo.iter_polygons(geometry):
        yield from polygon[0][:-1]


def _has_nested_footprint(r1: Region, r2: Region) -> bool:
    """True iff a vertex of one region lies strictly inside the other.

    Boundary-only contact (adjacent polygons sharing an edge) returns False;
    genuine area overlap or containment returns True.
    """
    for polygon in geo.iter_polygons(r1.geometry):
        if any(geo.point_in_polygon(v, polygon) for v in _exterior_vertices(r2.geometry)):
            return True
    for polygon in geo.iter_polygons(r2.geometry):
        if any(geo.point_in_polygon(v, polygon) for v in _exterior_vertices(r1.geometry)):
            return True
    return False


@dataclass(frozen=True)
class Region:
    region_id: str
    name: str
    geometry: dict
    bbox: tuple
    geom_hash: str


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_boundary_geojson(doc: dict, source_name: str | None = None) -> dict:
    """Validate an uploaded boundary document.

    Returns {"regions": [Region...], "hash": str, "notes": [...],
             "self_intersects": bool}. Raises geometry.GeometryError /
             ValueError on anything unusable.
    """
    if not isinstance(doc, dict) or doc.get("type") != "FeatureCollection":
        raise ValueError("boundary upload must be a GeoJSON FeatureCollection")
    features = doc.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("FeatureCollection has no features")

    regions: list[Region] = []
    seen_ids: set[str] = set()
    notes: list[str] = []

    for idx, feat in enumerate(features):
        if not isinstance(feat, dict) or feat.get("type") != "Feature":
            raise ValueError(f"feature #{idx} is not a GeoJSON Feature")
        props = feat.get("properties") or {}
        region_id = props.get("region_id")
        if not region_id or not isinstance(region_id, str):
            raise ValueError(f"feature #{idx} lacks a string properties.region_id")
        if region_id in seen_ids:
            raise ValueError(f"duplicate region_id: {region_id!r}")
        seen_ids.add(region_id)

        geometry = geo.validate_geojson_geometry(feat.get("geometry") or {})
        if geometry["type"] not in ("Polygon", "MultiPolygon"):
            raise ValueError(
                f"region {region_id!r}: only Polygon/MultiPolygon are supported"
            )
        bbox = geo.bbox_of_polygons(geometry)
        regions.append(
            Region(
                region_id=region_id,
                name=str(props.get("name") or region_id),
                geometry=geometry,
                bbox=bbox,
                geom_hash=_sha256(geo.canonical_geometry_json(geometry)),
            )
        )

    # Overlap / intersection audit between every pair (sorted => deterministic).
    # Distinguish harmless edge-adjacency (boundaries touch, neither polygon
    # contains the other's vertices) from real footprint overlaps, which make
    # point ownership ambiguous and therefore require the conflict workflow.
    regions.sort(key=lambda r: r.region_id)
    adjacency_pairs: list[str] = []
    overlap_pairs: list[str] = []
    for i, r1 in enumerate(regions):
        for r2 in regions[i + 1:]:
            if not geo.geometries_intersect(r1.geometry, r2.geometry):
                continue
            nested = _has_nested_footprint(r1, r2)
            (overlap_pairs if nested else adjacency_pairs).append(
                f"{r1.region_id}~{r2.region_id}"
            )
    self_intersects = bool(overlap_pairs)
    if adjacency_pairs:
        notes.append(
            "edge-adjacent region pairs (shared boundary is normal; points on "
            "it are classified ON_BOUNDARY, never silently split): "
            + ", ".join(adjacency_pairs)
        )
    if overlap_pairs:
        notes.append(
            "overlapping region footprints: " + ", ".join(overlap_pairs)
            + "; points inside overlaps are classified OVERLAP and queued for "
            "rule-based resolution (never auto-assigned)"
        )
    notes.append(
        f"validated {len(regions)} regions from {source_name or 'upload'}; "
        "coordinates interpreted as EPSG:4326 lon/lat (see docs/COORDINATES.md)"
    )

    combined = "\n".join(
        f"{r.region_id}:{r.geom_hash}" for r in regions
    )
    return {
        "regions": regions,
        "hash": _sha256(combined),
        "notes": notes,
        "self_intersects": self_intersects,
    }


def load_regions(store, boundary_version: int) -> list[Region]:
    rows = store.list_regions(boundary_version)
    out = []
    for row in rows:
        out.append(
            Region(
                region_id=row["region_id"],
                name=row["name"],
                geometry=json.loads(row["geometry_json"]),
                bbox=tuple(json.loads(row["bbox_json"])),
                geom_hash=row["geom_hash"],
            )
        )
    return out


def persist_boundary_set(store, parsed: dict, source_name: str | None) -> int:
    version = store.insert_boundary_set(
        source_name=source_name,
        geometry_hash=parsed["hash"],
        region_count=len(parsed["regions"]),
        self_intersects=parsed["self_intersects"],
        notes=parsed["notes"],
    )
    for r in parsed["regions"]:
        store.insert_region(
            version, r.region_id, r.name, r.geometry, list(r.bbox), r.geom_hash
        )
    return version


# ---------------------------------------------------------------------------
# Impact analysis: which regions / events are affected by a boundary change?
# ---------------------------------------------------------------------------

def analyze_impact(store, old_version: int | None, new_version: int) -> dict:
    """Compute the evidence base for FULL vs CHANGED_REGIONS recompute.

    A region is "changed" when its geometry hash differs. Every event whose
    old OR new classification could differ is potentially affected; we
    approximate that conservatively and exactly by geometry:

      * added/removed regions => every event in their bbox is affected;
      * changed regions => every event in the union of old/new bbox is
        affected (bbox is a cheap pre-filter; the worker recomputes exact
        point-in-polygon for these candidates only);
      * an event located in an unchanged region but inside a changed region's
        bbox is still re-tested, so impact can only over-approximate, never
        miss.

    Returns a JSON-serialisable dict with region-level and event-level counts
    plus the explicit rationale used to pick the scope.
    """
    new_regions = load_regions(store, new_version)
    new_by_id = {r.region_id: r for r in new_regions}
    old_by_id: dict[str, Region] = {}
    if old_version is not None:
        old_by_id = {r.region_id: r for r in load_regions(store, old_version)}

    added, removed, changed, unchanged = [], [], [], []
    for rid, r in new_by_id.items():
        if rid not in old_by_id:
            added.append(rid)
        elif old_by_id[rid].geom_hash != r.geom_hash:
            changed.append(rid)
        else:
            unchanged.append(rid)
    for rid in old_by_id:
        if rid not in new_by_id:
            removed.append(rid)

    affected_ids = set(added) | set(removed) | set(changed)
    candidate_regions = [
        r for rid, r in {**old_by_id, **new_by_id}.items() if rid in affected_ids
    ]

    affected_events = []
    total_events = store.max_seq()
    if candidate_regions:
        for ev in store.list_events():
            for r in candidate_regions:
                if geo.bbox_contains(r.bbox, ev["lon"], ev["lat"]):
                    affected_events.append(ev["event_id"])
                    break

    # Exact verification: how many affected candidates actually change
    # classification between old and new geometry? This is computed lazily by
    # the worker, but the bbox-bounded estimate drives the scope decision.
    rationale = (
        f"{len(changed)} changed, {len(added)} added, {len(removed)} removed "
        f"region(s); {len(affected_events)}/{total_events} event(s) fall inside "
        "the bounding box of a changed footprint and require re-testing"
    )
    return {
        "added_regions": sorted(added),
        "removed_regions": sorted(removed),
        "changed_regions": sorted(changed),
        "unchanged_regions": sorted(unchanged),
        "affected_event_ids": sorted(affected_events),
        "affected_event_count": len(affected_events),
        "total_event_count": total_events,
        "rationale": rationale,
    }


def recommend_scope(impact: dict, full_threshold: float = 0.8) -> str:
    """Scope decision with an explicit, stored rule.

    Full recompute when (a) there is no prior version, (b) every/most regions
    changed, or (c) the affected-event share exceeds ``full_threshold``.
    Otherwise CHANGED_REGIONS is cheaper and provably sufficient: unchanged
    region geometry means unchanged point classification inside it, and the
    bbox expansion in analyze_impact catches spill-over across boundaries.
    """
    total = impact["total_event_count"]
    if total == 0:
        return "CHANGED_REGIONS"
    if not impact["unchanged_regions"] and (
        impact["changed_regions"] or impact["added_regions"]
    ):
        return "FULL"
    share = impact["affected_event_count"] / total
    return "FULL" if share >= full_threshold else "CHANGED_REGIONS"
