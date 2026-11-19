"""HTTP API smoke tests: server starts in-process and every UI flow is hit."""
import http.client
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pointwise.seed_demo import BOUNDARY_V1, BOUNDARY_V2
from pointwise.web.server import PointwiseServer


class WebApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = os.path.join(cls.tmp.name, "web.db")
        cls.httpd = PointwiseServer(("127.0.0.1", 0), cls.db, log=lambda *_: None)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=5)
        cls.tmp.cleanup()

    def _req(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        data = json.dumps(body).encode() if body is not None else None
        conn.request(method, path, data,
                     {"Content-Type": "application/json"} if data else {})
        resp = conn.getresponse()
        payload = json.loads(resp.read().decode())
        conn.close()
        return resp.status, payload

    def test_full_console_flow(self):
        # 0. console HTML is served
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("GET", "/")
        self.assertEqual(conn.getresponse().status, 200)
        conn.close()

        # 1. upload + activate v1, then ingest events (incl. edge/overlap).
        st, v1 = self._req("POST", "/api/boundaries", BOUNDARY_V1)
        self.assertEqual(st, 201)
        st, _ = self._req(
            "POST", f"/api/boundaries/{v1['boundary_version']}/activate", {})
        self.assertEqual(st, 200)
        for eid, lon, lat in [
            ("w1", 116.2, 40.0), ("w2", 116.5, 40.0),
            ("w3", 116.9, 40.12), ("w4", 120.0, 40.0),
        ]:
            st, _ = self._req("POST", "/api/events",
                              {"event_id": eid, "lon": lon, "lat": lat})
            self.assertEqual(st, 201)

        # duplicate event rejected
        st, err = self._req("POST", "/api/events",
                            {"event_id": "w1", "lon": 1, "lat": 1})
        self.assertEqual(st, 409)
        self.assertIn("duplicate", err["error"])

        # out-of-range coordinate rejected
        st, err = self._req("POST", "/api/events",
                            {"event_id": "bad", "lon": 200, "lat": 1})
        self.assertEqual(st, 400)

        # 2. status shows active versions and UNLOCATED counts
        st, status = self._req("GET", "/api/status")
        self.assertEqual(status["active_versions"]["boundary_version"],
                         v1["boundary_version"])
        self.assertIn("UNLOCATED", status["unlocated_counts"])

        # 3. propose + approve a MANUAL rule; edge/overlap become conflicts
        st, rule = self._req("POST", "/api/rules",
                             {"on_boundary": "MANUAL", "overlap": "MANUAL"})
        self.assertEqual(st, 201)
        st, _ = self._req(
            "POST", f"/api/rules/{rule['rule_version']}/approve", {})
        self.assertEqual(st, 200)
        # Re-ingest triggers nothing; instead run a fresh event to exercise it,
        # and verify conflicts endpoint lists the v1 MANUAL backfill via worker
        # by creating an event after approval:
        st, _ = self._req("POST", "/api/events",
                          {"event_id": "w5", "lon": 116.5, "lat": 40.0})
        st, conflicts = self._req(
            "GET", "/api/conflicts?state=OPEN&boundary_version="
            + str(v1["boundary_version"]))
        self.assertEqual(st, 200)
        self.assertTrue(any(c["event_id"] == "w5" for c in conflicts))
        target = next(c for c in conflicts if c["event_id"] == "w5")
        # resolve with an invalid region -> 400, then a valid candidate -> 200
        st, err = self._req(
            "POST", f"/api/conflicts/{target['conflict_id']}/resolve",
            {"region_id": "NOPE"})
        self.assertEqual(st, 400)
        st, resolved = self._req(
            "POST", f"/api/conflicts/{target['conflict_id']}/resolve",
            {"region_id": "B"})
        self.assertEqual(st, 200)
        self.assertEqual(resolved["chosen_region_id"], "B")

        # 4. upload v2, create job with explicit scope, run, activate
        st, v2 = self._req("POST", "/api/boundaries", BOUNDARY_V2)
        st, job = self._req(
            "POST", f"/api/boundaries/{v2['boundary_version']}/job",
            {"scope": "FULL"})
        self.assertEqual(job["scope"], "FULL")
        st, ran = self._req("POST", f"/api/jobs/{job['job_id']}/run", {})
        self.assertEqual(ran["state"], "COMPLETED")
        # cannot interrupt a completed job
        st, err = self._req(
            "POST", f"/api/jobs/{job['job_id']}/interrupt", {})
        self.assertEqual(st, 409)
        st, act = self._req(
            "POST", f"/api/boundaries/{v2['boundary_version']}/activate", {})
        self.assertEqual(act["state"], "ACTIVE")

        # 5. report + comparison + reproduction
        st, report = self._req("POST", "/api/reports", {
            "boundary_version": v2["boundary_version"],
            "compare_version": v1["boundary_version"]})
        self.assertEqual(st, 201)
        self.assertIsNotNone(report["comparison"])
        st, repro = self._req(
            "POST", f"/api/reports/{report['report_id']}/reproduce", {})
        self.assertEqual(st, 200)
        self.assertTrue(repro["hash_matches"])

        # 6. bad upload is a 400, not a crash
        st, err = self._req("POST", "/api/boundaries",
                            {"type": "Feature", "features": []})
        self.assertEqual(st, 400)
        st, err = self._req("GET", "/api/nope")
        self.assertEqual(st, 404)


if __name__ == "__main__":
    unittest.main()
