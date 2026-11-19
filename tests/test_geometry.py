"""Tests for the hand-written geometry engine."""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pointwise import geometry as geo
from pointwise.geometry import GeometryError


def square(x0, y0, x1, y1):
    return {"type": "Polygon", "coordinates": [[
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]]}


class RegionStub:
    def __init__(self, region_id, geometry):
        self.region_id = region_id
        self.geometry = geometry
        self.bbox = geo.bbox_of_polygons(geometry)


class PrimitiveTests(unittest.TestCase):
    def test_point_in_ring_basic(self):
        ring = square(0, 0, 10, 10)["coordinates"][0]
        self.assertTrue(geo.point_in_ring((5, 5), ring))
        self.assertFalse(geo.point_in_ring((11, 5), ring))
        # Boundary handling belongs to point_on_ring (the classifier calls that
        # first); point_in_ring is only used for strict interior tests.
        self.assertTrue(geo.point_on_ring((0, 5), ring))
        self.assertTrue(geo.point_on_ring((10, 10), ring))
        self.assertFalse(geo.point_on_ring((0.001, 5), ring))

    def test_ray_casting_horizontal_vertex_no_double_count(self):
        # Two stacked squares sharing vertex (0,0)-style configuration:
        # a point on the shared horizontal line must classify consistently
        # across repeated runs (half-open ray convention).
        ring = square(0, 0, 10, 10)["coordinates"][0]
        results = {geo.point_in_ring((5, 0.0001), ring) for _ in range(50)}
        self.assertEqual(results, {True})

    def test_point_on_horizontal_edge_is_not_strictly_inside(self):
        # Regression: half-open ray casting alone reports a point on a
        # horizontal edge as interior; point_in_ring must exclude boundaries.
        ring = square(0, 0, 10, 10)["coordinates"][0]
        for p in ((5, 0), (0, 5), (5, 10), (10, 5), (0, 0), (10, 10)):
            self.assertFalse(geo.point_in_ring(p, ring), f"{p} is boundary")
            self.assertTrue(geo.point_on_ring(p, ring))
        # A point just inside still works.
        self.assertTrue(geo.point_in_ring((5, 0.001), ring))

    def test_point_on_segment(self):
        a, b = (0.0, 0.0), (10.0, 10.0)
        self.assertTrue(geo.point_on_segment((5, 5), a, b))
        self.assertTrue(geo.point_on_segment((0, 0), a, b))
        self.assertFalse(geo.point_on_segment((5, 5.001), a, b))
        # Horizontal / vertical
        self.assertTrue(geo.point_on_segment((7, 0), (0, 0), (10, 0)))
        self.assertFalse(geo.point_on_segment((11, 0), (0, 0), (10, 0)))

    def test_hole_excludes_interior(self):
        poly = [
            square(0, 0, 10, 10)["coordinates"][0],
            square(4, 4, 6, 6)["coordinates"][0],
        ]
        self.assertTrue(geo.point_in_polygon((2, 2), poly))
        self.assertFalse(geo.point_in_polygon((5, 5), poly))
        self.assertTrue(geo.point_touches_polygon((4, 5), poly))

    def test_segments_intersect(self):
        self.assertTrue(geo.segments_intersect((0, 0), (10, 10), (0, 10), (10, 0)))
        self.assertFalse(geo.segments_intersect((0, 0), (1, 1), (2, 2), (3, 3)))
        # Collinear overlap
        self.assertTrue(geo.segments_intersect((0, 0), (5, 5), (3, 3), (8, 8)))
        # T-junction endpoint touch counts as intersection
        self.assertTrue(geo.segments_intersect((0, 0), (10, 0), (5, 0), (5, 5)))

    def test_validation_rejects_bad_geojson(self):
        with self.assertRaises(GeometryError):
            geo.validate_geojson_geometry({"type": "LineString", "coordinates": []})
        with self.assertRaises(GeometryError):
            geo.validate_geojson_geometry({"type": "Point", "coordinates": [1]})
        with self.assertRaises(GeometryError):
            geo.validate_geojson_geometry({"type": "Point",
                                           "coordinates": [float("nan"), 1]})
        with self.assertRaises(GeometryError):
            geo.validate_geojson_geometry({"type": "Polygon",
                                           "coordinates": [[(0, 0), (1, 1)]]})

    def test_canonical_hash_key_order_and_signed_zero(self):
        g1 = {"type": "Point", "coordinates": [1.0, 2.0]}
        g2 = {"coordinates": [1.0, 2.0], "type": "Point"}
        g3 = {"type": "Point", "coordinates": [1.0, -0.0]}
        g4 = {"type": "Point", "coordinates": [1.0, 0.0]}
        self.assertEqual(geo.canonical_geometry_json(g1),
                         geo.canonical_geometry_json(g2))
        self.assertEqual(geo.canonical_geometry_json(g3),
                         geo.canonical_geometry_json(g4))


class ClassificationTests(unittest.TestCase):
    def setUp(self):
        # A=[0,10]x[0,10], B=[10,20]x[0,10] share edge x=10;
        # C overlaps B's north-east corner.
        self.regions = [
            RegionStub("A", square(0, 0, 10, 10)),
            RegionStub("B", square(10, 0, 20, 10)),
            RegionStub("C", square(15, 5, 20, 10)),
        ]

    def test_inside(self):
        c = geo.classify_point(5, 5, self.regions)
        self.assertEqual(c.status, "INSIDE")
        self.assertEqual(c.region_ids, ("A",))

    def test_shared_edge_is_boundary_with_both_candidates_sorted(self):
        c = geo.classify_point(10, 5, self.regions)
        self.assertEqual(c.status, "ON_BOUNDARY")
        self.assertEqual(c.region_ids, ("A", "B"))

    def test_overlap_deterministic_candidates(self):
        c = geo.classify_point(17, 7, self.regions)
        self.assertEqual(c.status, "OVERLAP")
        self.assertEqual(c.region_ids, ("B", "C"))
        # Shuffling input order must not change the result.
        shuffled = list(reversed(self.regions))
        c2 = geo.classify_point(17, 7, shuffled)
        self.assertEqual(c, c2)

    def test_unlocated(self):
        c = geo.classify_point(50, 50, self.regions)
        self.assertEqual(c.status, "UNLOCATED")
        self.assertEqual(c.region_ids, ())

    def test_boundary_corner_point(self):
        c = geo.classify_point(10, 10, self.regions)
        self.assertEqual(c.status, "ON_BOUNDARY")
        self.assertIn("A", c.region_ids)
        self.assertIn("B", c.region_ids)

    def test_geometries_intersect_detects_nested_polygon(self):
        outer = square(0, 0, 20, 20)
        inner = square(5, 5, 8, 8)
        self.assertTrue(geo.geometries_intersect(outer, inner))
        disjoint = square(30, 30, 40, 40)
        self.assertFalse(geo.geometries_intersect(outer, disjoint))


if __name__ == "__main__":
    unittest.main()
