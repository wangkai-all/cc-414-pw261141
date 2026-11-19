"""Deterministic planar point-in-polygon geometry, implemented from scratch.

Coordinate system contract (see docs/COORDINATES.md):
  * All coordinates are geographic lon/lat in EPSG:4326 (WGS 84), axis order
    (longitude, latitude), degrees.
  * All tests below are *planar* tests on the raw degree values: ray casting
    and segment intersection treat lon/lat as Cartesian x/y. No projection or
    datum transformation is performed.
  * Tolerances are therefore expressed in degrees, not metres. 1e-9 degree is
    ~0.1 mm at the equator; it is only an epsilon for floating point edge
    classification, NOT a ground-distance buffer.
  * Polygons crossing the antimeridian (lon +/- 180) are not unwrapped; do not
    author such polygons without splitting them first.

Every predicate is deterministic: given identical inputs it returns identical
outputs on every run, and tie-breaking never depends on set/dict iteration
order (callers sort candidate ids).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

EPS = 1e-9
Point = tuple[float, float]
Ring = list[Point]


class GeometryError(ValueError):
    """Raised for malformed input geometry."""


# ---------------------------------------------------------------------------
# Validation / canonicalisation
# ---------------------------------------------------------------------------

def _as_point(coord) -> Point:
    if not coords_ok(coord):
        raise GeometryError("coordinate must be a sequence of numbers")
    if len(coord) < 2:
        raise GeometryError("coordinate needs at least x and y")
    try:
        x, y = float(coord[0]), float(coord[1])
    except (TypeError, ValueError) as exc:
        raise GeometryError(f"non-numeric coordinate: {coord!r}") from exc
    if not (math.isfinite(x) and math.isfinite(y)):
        raise GeometryError(f"non-finite coordinate: {coord!r}")
    return x, y


def coords_ok(coord) -> bool:
    return isinstance(coord, (list, tuple))


def _as_ring(ring) -> Ring:
    if not isinstance(ring, list) or len(ring) < 4:
        raise GeometryError("a linear ring needs >= 4 positions")
    pts = [_as_point(c) for c in ring]
    if pts[0] != pts[-1]:
        raise GeometryError("linear ring must be closed (first == last position)")
    if len(set(pts[:-1])) < 3:
        raise GeometryError("ring needs at least 3 distinct positions")
    return pts


def _as_polygon(poly) -> list[Ring]:
    """Polygon -> [exterior_ring, hole_ring, ...], all closed point lists."""
    if not isinstance(poly, list) or not poly:
        raise GeometryError("polygon needs at least an exterior ring")
    return [_as_ring(ring) for ring in poly]


def validate_geojson_geometry(geometry: dict) -> dict:
    """Validate a (sub-set of) GeoJSON geometry, return a normalised copy.

    Supported types: Point, Polygon, MultiPolygon. Anything else raises.
    Coordinates are copied as plain float tuples; no CRS member is honoured
    (the project only speaks EPSG:4326, see docs/COORDINATES.md).
    """
    if not isinstance(geometry, dict):
        raise GeometryError("geometry must be an object")
    gtype = geometry.get("type")
    raw = geometry.get("coordinates")
    if gtype == "Point":
        return {"type": "Point", "coordinates": list(_as_point(raw))}
    if gtype == "Polygon":
        return {"type": "Polygon", "coordinates": [_as_ring(r) for r in raw]}
    if gtype == "MultiPolygon":
        if not isinstance(raw, list) or not raw:
            raise GeometryError("MultiPolygon needs at least one polygon")
        return {
            "type": "MultiPolygon",
            "coordinates": [[_as_ring(r) for r in poly] for poly in raw],
        }
    raise GeometryError(f"unsupported geometry type: {gtype!r}")


def iter_polygons(geometry: dict):
    """Yield polygons (each a list of rings) from a validated geometry."""
    gtype = geometry["type"]
    if gtype == "Polygon":
        yield _as_polygon(geometry["coordinates"])
    elif gtype == "MultiPolygon":
        for poly in geometry["coordinates"]:
            yield _as_polygon(poly)
    else:  # pragma: no cover - guarded by validate
        raise GeometryError(f"cannot iterate polygons of {gtype}")


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def bbox_of_polygons(geometry: dict) -> tuple[float, float, float, float]:
    """Return (minx, miny, maxx, maxy) across all polygon vertices."""
    xs, ys = [], []
    for poly in iter_polygons(geometry):
        for ring in poly:
            for x, y in ring:
                xs.append(x)
                ys.append(y)
    if not xs:
        raise GeometryError("empty geometry has no bbox")
    return min(xs), min(ys), max(xs), max(ys)


def bbox_contains(bbox, x: float, y: float) -> bool:
    minx, miny, maxx, maxy = bbox
    return (minx - EPS) <= x <= (maxx + EPS) and (miny - EPS) <= y <= (maxy + EPS)


def point_on_segment(p: Point, a: Point, b: Point, eps: float = EPS) -> bool:
    """True iff p lies on closed segment a-b (collinear and within extent)."""
    px, py = p
    ax, ay = a
    bx, by = b
    cross = (bx - ax) * (py - ay) - (by - ay) * (px - ax)
    if abs(cross) > eps * max(1.0, abs(bx - ax), abs(by - ay)):
        return False
    return (min(ax, bx) - eps) <= px <= (max(ax, bx) + eps) and (
        min(ay, by) - eps
    ) <= py <= (max(ay, by) + eps)


def point_on_ring(p: Point, ring: Ring, eps: float = EPS) -> bool:
    for i in range(len(ring) - 1):
        if point_on_segment(p, ring[i], ring[i + 1], eps):
            return True
    return False


def point_in_ring(p: Point, ring: Ring) -> bool:
    """Strict interior test for a ring via half-open ray casting.

    Boundary points return False: any point lying on a ring segment (including
    horizontal edges, where the half-open ray convention alone would
    misclassify a boundary point as interior) is excluded explicitly. Boundary
    handling is the caller's job (see classify_point).
    """
    if point_on_ring(p, ring):
        return False
    x, y = p
    inside = False
    n = len(ring) - 1  # ring is closed
    for i in range(n):
        ax, ay = ring[i]
        bx, by = ring[i + 1]
        # Edge straddles the horizontal ray (half-open interval at the lower
        # end, so vertices shared by two edges are counted exactly once).
        if (ay > y) != (by > y):
            x_intersect = ax + (y - ay) * (bx - ax) / (by - ay)
            if x < x_intersect:
                inside = not inside
    return inside


def point_in_polygon(p: Point, polygon: list[Ring]) -> bool:
    """Strict interior of a polygon with holes."""
    exterior, *holes = polygon
    if not point_in_ring(p, exterior):
        return False
    return not any(point_in_ring(p, hole) for hole in holes)


def point_touches_polygon(p: Point, polygon: list[Ring]) -> bool:
    """True iff p lies on any boundary segment of the polygon."""
    return any(point_on_ring(p, ring) for ring in polygon)


# ---------------------------------------------------------------------------
# Segment / ring intersection (boundary-change detection)
# ---------------------------------------------------------------------------

def segments_intersect(a: Point, b: Point, c: Point, d: Point) -> bool:
    """Proper or improper intersection of closed segments a-b and c-d."""
    def cross(u, v):
        return u[0] * v[1] - u[1] * v[0]

    def sub(u, v):
        return (u[0] - v[0], u[1] - v[1])

    r = sub(b, a)
    s = sub(d, c)
    rxs = cross(r, s)
    qp = sub(c, a)
    if abs(rxs) <= EPS:
        if abs(cross(qp, r)) > EPS:
            return False
        # Collinear: check projection overlap.
        return max(min(a[0], b[0]), min(c[0], d[0])) <= min(
            max(a[0], b[0]), max(c[0], d[0])
        ) + EPS and max(min(a[1], b[1]), min(c[1], d[1])) <= min(
            max(a[1], b[1]), max(d[1], c[1])
        ) + EPS
    t = cross(qp, s) / rxs
    u = cross(qp, r) / rxs
    return (-EPS) <= t <= 1.0 + EPS and (-EPS) <= u <= 1.0 + EPS


def rings_intersect(r1: Ring, r2: Ring) -> bool:
    """True iff any boundary segment of r1 crosses/touches one of r2."""
    for i in range(len(r1) - 1):
        a, b = r1[i], r1[i + 1]
        for j in range(len(r2) - 1):
            if segments_intersect(a, b, r2[j], r2[j + 1]):
                return True
    return False


def geometries_intersect(g1: dict, g2: dict) -> bool:
    """Boundary intersection test between two validated polygon geometries.

    Used to detect overlapping region footprints (which create ambiguous
    point ownership). Interior containment without boundary contact is also
    flagged by sampling vertices, because a polygon fully inside another never
    shares a boundary segment.
    """
    for p1 in iter_polygons(g1):
        for p2 in iter_polygons(g2):
            for r1 in p1:
                for r2 in p2:
                    if rings_intersect(r1, r2):
                        return True
            # Nested polygon: a vertex of one sits strictly inside the other.
            for v in p2[0][:-1]:
                if point_in_polygon(v, p1):
                    return True
            for v in p1[0][:-1]:
                if point_in_polygon(v, p2):
                    return True
    return False


# ---------------------------------------------------------------------------
# Point classification against a region set
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Classification:
    status: str               # INSIDE | ON_BOUNDARY | OVERLAP | UNLOCATED
    region_ids: tuple         # deterministic, ascending candidate region ids

    def as_dict(self) -> dict:
        return {"status": self.status, "region_ids": list(self.region_ids)}


def classify_point(
    x: float,
    y: float,
    regions: list,
) -> Classification:
    """Classify one point against a sorted list of region objects.

    Each region object exposes .region_id (str), .geometry (validated
    GeoJSON dict) and .bbox. The result order is fully deterministic:
    candidates are gathered, then sorted by region id. Nothing here samples
    hash order or "picks a random region".

    Statuses:
      INSIDE       - exactly one region's polygon interior contains the point
      ON_BOUNDARY  - point lies on one or more region boundary segments
      OVERLAP      - point is inside two or more region interiors
      UNLOCATED    - point is inside / on no region
    """
    p = (x, y)
    interiors: list[str] = []
    boundaries: list[str] = []
    for region in sorted(regions, key=lambda r: r.region_id):
        if not bbox_contains(region.bbox, x, y):
            continue
        for polygon in iter_polygons(region.geometry):
            if point_touches_polygon(p, polygon):
                boundaries.append(region.region_id)
                break
        else:  # no boundary hit: test interior
            for polygon in iter_polygons(region.geometry):
                if point_in_polygon(p, polygon):
                    interiors.append(region.region_id)
                    break

    interiors = sorted(set(interiors))
    boundaries = sorted(set(boundaries))

    if boundaries:
        # A point on a shared edge has every adjacent region as a candidate.
        return Classification("ON_BOUNDARY", tuple(sorted(set(boundaries + interiors))))
    if len(interiors) > 1:
        return Classification("OVERLAP", tuple(interiors))
    if len(interiors) == 1:
        return Classification("INSIDE", tuple(interiors))
    return Classification("UNLOCATED", ())


# ---------------------------------------------------------------------------
# Geometry hashing (impact analysis)
# ---------------------------------------------------------------------------

def canonical_geometry_json(geometry: dict) -> str:
    """Stable textual form of a geometry for content hashing."""
    import json

    def walk(obj):
        if isinstance(obj, dict):
            return {k: walk(obj[k]) for k in sorted(obj)}
        if isinstance(obj, list):
            return [walk(v) for v in obj]
        if isinstance(obj, float):
            # Normalise -0.0 -> 0.0 so equivalent text hashes match.
            return 0.0 if obj == 0.0 else obj
        return obj

    return json.dumps(walk(geometry), separators=(",", ":"), sort_keys=True)
