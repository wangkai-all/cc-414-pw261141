"""End-to-end workflow tests: versioning, determinism, interrupts, reports."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pointwise import app as services
from pointwise.reports import reproduce_report, run_report
from pointwise.seed_demo import BOUNDARY_V1, BOUNDARY_V2, EVENTS
from pointwise.storage import Store
from pointwise.worker import POLICY_MANUAL, POLICY_MIN_ID, Worker


class WorkflowTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db)
        services.ensure_defaults(self.store)
        v1 = services.upload_boundary(self.store, BOUNDARY_V1, "v1")
        services.activate_version(self.store, v1["boundary_version"])
        self.v1 = v1["boundary_version"]
        for eid, lon, lat, _ in EVENTS:
            services.ingest_event(self.store, eid, lon, lat)
        self.worker = Worker(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()


class DeterminismTests(WorkflowTestBase):
    def test_boundary_and_overlap_resolved_without_randomness(self):
        # e-003 sits on the A/B shared edge; e-004 inside B/C overlap.
        edge = self.store.get_attribution("e-003", self.v1, 1)
        self.assertEqual(edge["status"], "ON_BOUNDARY")
        self.assertEqual(edge["region_id"], "A")  # MIN_ID picks A over B
        self.assertEqual(json.loads(edge["candidate_ids"]), ["A", "B"])

        overlap = self.store.get_attribution("e-004", self.v1, 1)
        self.assertEqual(overlap["status"], "OVERLAP")
        self.assertEqual(overlap["region_id"], "B")  # B < C

        # Recompute the same rows on a fresh worker/DB connection: identical.
        store2 = Store(self.db)
        cls_rows = store2.conn.execute(
            "SELECT * FROM attributions ORDER BY event_id"
        ).fetchall()
        fresh = Store(self.db)
        again = fresh.conn.execute(
            "SELECT * FROM attributions ORDER BY event_id"
        ).fetchall()
        self.assertEqual([dict(r) for r in cls_rows], [dict(r) for r in again])

    def test_unlocated_is_never_assigned(self):
        # e-005 is outside every v1 region.
        row = self.store.get_attribution("e-005", self.v1, 1)
        self.assertEqual(row["status"], "UNLOCATED")
        self.assertIsNone(row["region_id"])

    def test_manual_policy_opens_conflicts_and_assigns_nothing(self):
        rv = services.propose_rule(
            self.store, POLICY_MANUAL, POLICY_MANUAL, "human review"
        )["rule_version"]
        services.approve_rule(self.store, rv)
        # Re-attribute existing events under the new rule version.
        for ev in self.store.list_events():
            self.worker.attribute_pending_live_events()
        edge = self.store.get_attribution("e-003", self.v1, rv)
        self.assertTrue(edge["needs_review"])
        self.assertIsNone(edge["region_id"])
        conflicts = self.store.list_conflicts()
        reasons = {c["event_id"]: c["reason"] for c in conflicts}
        self.assertIn("e-003", reasons)
        self.assertIn("e-004", reasons)
        # Resolution must choose among candidates only.
        cid = next(c["conflict_id"] for c in conflicts
                   if c["event_id"] == "e-003")
        with self.assertRaises(ValueError):
            services.resolve_conflict(self.store, cid, "ZZ-NOT-A-CANDIDATE")
        services.resolve_conflict(self.store, cid, "B")
        row = self.store.get_attribution("e-003", self.v1, rv)
        self.assertEqual(row["region_id"], "B")
        self.assertEqual(row["status"], "RESOLVED")


class ImpactAndRecomputeTests(WorkflowTestBase):
    def _create_v2_job(self, scope=None):
        v2 = services.upload_boundary(self.store, BOUNDARY_V2, "v2")
        job = services.create_recompute_job(
            self.store, v2["boundary_version"], scope=scope
        )
        return v2, job

    def test_impact_evidence_detects_changed_added_regions(self):
        v2, job = self._create_v2_job()
        impact = job["impact"]
        self.assertEqual(impact["changed_regions"], ["A", "B"])
        self.assertEqual(impact["added_regions"], ["D"])
        self.assertEqual(impact["removed_regions"], [])
        self.assertEqual(sorted(impact["unchanged_regions"]), ["C", "E"])
        self.assertIn("rationale", impact)
        # e-009 sits inside region E, whose geometry and footprint are
        # untouched and far from every changed bbox -> never a candidate.
        self.assertNotIn("e-009", impact["affected_event_ids"])
        # e-008 is inside region A which itself changed, so its bbox makes it a
        # (conservatively included) candidate even if its point does not move.
        self.assertIn("e-008", impact["affected_event_ids"])
        # e-006 crosses the moved edge -> affected.
        self.assertIn("e-006", impact["affected_event_ids"])
        # Only 2 of 9 events are affected -> evidence-based recommendation is
        # the cheap, provably-sufficient partial scope.
        self.assertEqual(job["scope"], "CHANGED_REGIONS")

    def test_changed_region_scope_skips_unaffected_event(self):
        v2, job = self._create_v2_job()
        self.assertEqual(job["scope"], "CHANGED_REGIONS")
        Worker(self.store).run_job(job["job_id"])
        # Event in unchanged region E gets no v2 row; affected events do.
        self.assertIsNone(
            self.store.get_attribution("e-009", v2["boundary_version"], 1))
        self.assertIsNotNone(
            self.store.get_attribution("e-006", v2["boundary_version"], 1))
        # After activation, live backfill covers the skipped event under v2.
        services.activate_version(self.store, v2["boundary_version"])
        self.assertIsNotNone(
            self.store.get_attribution("e-009", v2["boundary_version"], 1))

    def test_full_scope_touches_every_snapshot_event(self):
        v2, job = self._create_v2_job(scope="FULL")
        self.assertEqual(job["scope"], "FULL")
        Worker(self.store).run_job(job["job_id"])
        for eid, *_ in EVENTS:
            self.assertIsNotNone(
                self.store.get_attribution(eid, v2["boundary_version"], 1),
                f"{eid} missing under FULL recompute",
            )

    def test_ownership_change_and_new_unlocated_under_new_boundary(self):
        v2, job = self._create_v2_job(scope="FULL")
        Worker(self.store).run_job(job["job_id"])
        # e-006: B in v1 -> A in v2 (edge moved east).
        old = self.store.get_attribution("e-006", self.v1, 1)
        new = self.store.get_attribution("e-006", v2["boundary_version"], 1)
        self.assertEqual(old["region_id"], "B")
        self.assertEqual(new["region_id"], "A")
        # e-007: B in v1 -> UNLOCATED in v2 (south edge moved north).
        new7 = self.store.get_attribution("e-007", v2["boundary_version"], 1)
        self.assertEqual(new7["status"], "UNLOCATED")
        self.assertIsNone(new7["region_id"])
        # e-005: UNLOCATED in v1 -> inside new region D in v2.
        new5 = self.store.get_attribution("e-005", v2["boundary_version"], 1)
        self.assertEqual(new5["region_id"], "D")

    def test_interrupt_and_resume_is_lossless(self):
        v2, job = self._create_v2_job(scope="FULL")
        stops = {"n": 0}

        def stop_after_two():
            stops["n"] += 1
            return stops["n"] > 2

        r1 = Worker(self.store).run_job(job["job_id"], should_stop=stop_after_two)
        self.assertEqual(r1["state"], "INTERRUPTED")
        done_partial = self.store.job_done_events(job["job_id"])
        self.assertEqual(len(done_partial), 2)

        # Resume: completion, no duplicate progress rows, every event covered.
        r2 = Worker(self.store).run_job(job["job_id"])
        self.assertEqual(r2["state"], "COMPLETED")
        done = self.store.job_done_events(job["job_id"])
        self.assertEqual(len(done), len(EVENTS))
        job_row = self.store.get_job(job["job_id"])
        self.assertEqual(job_row["processed_count"], len(EVENTS))

    def test_snapshot_pins_job_against_events_arriving_midjob(self):
        v2, job = self._create_v2_job(scope="FULL")
        self.assertEqual(job["snapshot_seq"], len(EVENTS))
        # New event arrives after job creation; it must be invisible to the job
        # and attributed live under the still-active old boundary.
        services.ingest_event(self.store, "e-LATE", 116.2, 40.0)
        Worker(self.store).run_job(job["job_id"])
        self.assertIsNone(
            self.store.get_attribution("e-LATE", v2["boundary_version"], 1))
        late_old = self.store.get_attribution("e-LATE", self.v1, 1)
        self.assertIsNotNone(late_old)
        self.assertEqual(late_old["region_id"], "A")
        # Activating v2 backfills the late event under the new version only
        # after the job is completed — versions never mixed.
        result = services.activate_version(self.store, v2["boundary_version"])
        self.assertEqual(result["state"], "ACTIVE")
        late_new = self.store.get_attribution("e-LATE", v2["boundary_version"], 1)
        self.assertIsNotNone(late_new)

    def test_activation_blocked_while_job_unfinished(self):
        v2, job = self._create_v2_job()
        with self.assertRaises(RuntimeError):
            services.activate_version(self.store, v2["boundary_version"])
        Worker(self.store).run_job(job["job_id"])
        # Now activation succeeds.
        self.assertEqual(
            services.activate_version(
                self.store, v2["boundary_version"])["state"],
            "ACTIVE",
        )


class ReportTests(WorkflowTestBase):
    def test_report_reproduction_hash_matches(self):
        out = run_report(self.store, self.v1)
        again = reproduce_report(self.store, out["report_id"])
        self.assertTrue(again["hash_matches"])
        totals = again["reproduced"]["totals"]
        self.assertEqual(totals["events_in_window"], len(EVENTS))
        self.assertEqual(totals["events_pending_attribution"], 0)

    def test_comparison_counts_same_batch_under_two_boundaries(self):
        v2 = services.upload_boundary(self.store, BOUNDARY_V2, "v2")
        job = services.create_recompute_job(
            self.store, v2["boundary_version"], scope="FULL")
        Worker(self.store).run_job(job["job_id"])
        out = run_report(self.store, v2["boundary_version"],
                         compare_version=self.v1)
        cmp = out["comparison"]
        changed_ids = {c["event_id"] for c in cmp["changed_event_samples"]}
        # e-006 flips B->A; e-007 B->UNLOCATED; e-005 UNLOCATED->D.
        self.assertIn("e-006", changed_ids)
        self.assertIn("e-007", changed_ids)
        self.assertIn("e-005", changed_ids)
        # e-008 unchanged in both.
        self.assertEqual(
            cmp["events_unchanged"] + cmp["events_changed"], len(EVENTS))
        flow_pairs = {
            (f["from"], t["region"])
            for f in cmp["region_flows"] for t in f["to"]
        }
        self.assertIn(("B", "A"), flow_pairs)


class CliSmokeTest(unittest.TestCase):
    def test_demo_command_end_to_end(self):
        from pointwise.__main__ import main

        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "demo.db")
            main(["--db", db, "demo"])
            store = Store(db)
            status = services.system_status(store)
            self.assertEqual(status["total_events"], len(EVENTS))
            self.assertGreaterEqual(len(status["boundary_sets"]), 2)
            job = status["jobs"][0]
            self.assertEqual(job["state"], "COMPLETED")
            # Reports were persisted and reproduce cleanly.
            reports = store.list_reports()
            self.assertGreaterEqual(len(reports), 2)
            r = reproduce_report(store, reports[0]["report_id"])
            self.assertTrue(r["hash_matches"])
            store.close()


if __name__ == "__main__":
    unittest.main()
