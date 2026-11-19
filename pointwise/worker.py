"""Spatial attribution engine and interruptible recompute worker.

The worker is the only component that runs point-in-polygon classification.
Ingestion and reporting never classify points themselves; they read the
attribution rows this module writes, so boundary-touching points, overlaps
and unlocated records get exactly one treatment across the whole system.

Determinism contract
--------------------
* classification itself is deterministic (see geometry.classify_point);
* rule resolution is a pure function of (classification, rule row) — there is
  no random tie-break. The automatic policy ``MIN_ID`` always chooses the
  lexicographically smallest candidate region id; ``MANUAL`` queues a
  conflict instead of guessing;
* an UNLOCATED event is never assigned to a region by any policy.

Version isolation
-----------------
A recompute job pins ``snapshot_seq`` and only ever reads events with
``seq <= snapshot_seq``. Events arriving after job creation keep flowing
through :meth:`attribute_pending_live_events`, which always uses the current
ACTIVE boundary, so the two versions can never be mixed inside one report.
"""
from __future__ import annotations

import json

from . import geometry as geo
from .boundaries import Region, load_regions
from .storage import utcnow

# Rule policy vocabulary (stored in rules.on_boundary / rules.overlap).
POLICY_MIN_ID = "MIN_ID"     # deterministic: smallest candidate region_id
POLICY_MANUAL = "MANUAL"     # queue a conflict, assign nothing automatically
VALID_POLICIES = {POLICY_MIN_ID, POLICY_MANUAL}

BATCH_COMMIT_EVENTS = 100


class RuleError(ValueError):
    pass


def validate_rule_policies(on_boundary: str, overlap: str) -> None:
    if on_boundary not in VALID_POLICIES:
        raise RuleError(f"invalid on_boundary policy {on_boundary!r}")
    if overlap not in VALID_POLICIES:
        raise RuleError(f"invalid overlap policy {overlap!r}")


def resolve_classification(classification: geo.Classification, rule) -> dict:
    """Pure rule function: geometry classification -> attribution decision.

    ``rule`` is a sqlite Row / mapping with on_boundary and overlap columns.
    Returns {status, region_id, needs_review, conflict_reason}.
    """
    status = classification.status
    candidates = list(classification.region_ids)

    if status == "UNLOCATED":
        # Explicitly unassigned; no policy may invent a region.
        return {"status": "UNLOCATED", "region_id": None,
                "needs_review": False, "conflict_reason": None}
    if status == "INSIDE":
        return {"status": "ATTRIBUTED", "region_id": candidates[0],
                "needs_review": False, "conflict_reason": None}

    policy = rule["on_boundary"] if status == "ON_BOUNDARY" else rule["overlap"]
    if policy == POLICY_MIN_ID:
        return {"status": status, "region_id": min(candidates),
                "needs_review": False, "conflict_reason": None}
    if policy == POLICY_MANUAL:
        reason = (
            "point lies on a shared boundary" if status == "ON_BOUNDARY"
            else "point lies inside overlapping region footprints"
        )
        return {"status": status, "region_id": None,
                "needs_review": True, "conflict_reason": reason}
    raise RuleError(f"unknown policy {policy!r}")  # pragma: no cover


class Worker:
    def __init__(self, store):
        self.store = store

    # -- live attribution --------------------------------------------------
    def _active_context(self):
        versions = self.store.get_active_versions()
        bv, rv = versions["boundary_version"], versions["rule_version"]
        if bv is None or rv is None:
            return None
        return bv, rv, load_regions(self.store, bv), self.store.get_rule(rv)

    def attribute_pending_live_events(self) -> dict:
        """Attribute every event missing a row under the ACTIVE versions.

        Safe to run concurrently with a recompute job: it only writes rows for
        the active (old) boundary version, while the job writes rows for its
        target (new/draft) version — different primary-key tuples.
        """
        ctx = self._active_context()
        if ctx is None:
            return {"attributed": 0, "reason": "no active boundary/rule yet"}
        bv, rv, regions, rule = ctx
        existing = {
            row["event_id"]
            for row in self.store.conn.execute(
                "SELECT event_id FROM attributions WHERE boundary_version=? "
                "AND rule_version=?",
                (bv, rv),
            )
        }
        n = 0
        conflicts = 0
        for ev in self.store.list_events():
            if ev["event_id"] in existing:
                continue
            if self._attribute_one(ev, regions, bv, rv, rule):
                conflicts += 1
            n += 1
        return {"attributed": n, "conflicts": conflicts,
                "boundary_version": bv, "rule_version": rv}

    def _attribute_one(self, ev, regions: list[Region], bv: int, rv: int,
                       rule) -> bool:
        cls = geo.classify_point(ev["lon"], ev["lat"], regions)
        decision = resolve_classification(cls, rule)
        self.store.upsert_attribution(
            event_id=ev["event_id"],
            boundary_version=bv,
            rule_version=rv,
            status=decision["status"],
            region_id=decision["region_id"],
            candidate_ids=cls.region_ids,
            needs_review=decision["needs_review"],
        )
        if decision["needs_review"]:
            self.store.open_conflict(
                ev["event_id"], bv, rv,
                decision["conflict_reason"], list(cls.region_ids)
            )
            return True
        return False

    # -- recompute jobs ----------------------------------------------------
    def run_job(self, job_id: int, should_stop=None) -> dict:
        """Execute (or resume) a recompute job until completion or interrupt.

        ``should_stop`` is an optional zero-arg callable polled after every
        event; the web UI's interrupt button also sets the
        ``interrupt_job_id`` meta key, which is checked the same way.
        """
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"unknown job {job_id}")
        if job["state"] == "COMPLETED":
            return {"job_id": job_id, "state": "COMPLETED", "already_done": True}
        if job["state"] == "FAILED":
            raise ValueError(f"job {job_id} previously failed: {job['error']}")

        impact = json.loads(job["impact_json"])
        target_regions = load_regions(self.store, job["boundary_version_to"])
        rule = self.store.get_rule(job["rule_version"])
        done = self.store.job_done_events(job_id)
        scoped_ids = set(impact.get("affected_event_ids") or [])

        self.store.conn.execute(
            "UPDATE jobs SET state='RUNNING', started_at=COALESCE(started_at,?) "
            "WHERE job_id=?",
            (utcnow(), job_id),
        )

        processed = job["processed_count"]
        conflict_count = job["conflict_count"]
        stopped = False
        self.store.conn.execute("BEGIN IMMEDIATE")
        try:
            # Materialise up front so batch COMMITs never interrupt an open
            # SELECT cursor (snapshot_seq bounds the working set).
            pending = self.store.conn.execute(
                "SELECT * FROM events WHERE seq<=? ORDER BY seq",
                (job["snapshot_seq"],),
            ).fetchall()
            for ev in pending:
                if job["scope"] == "CHANGED_REGIONS" and ev["event_id"] not in scoped_ids:
                    continue
                if ev["event_id"] in done:
                    continue
                if self._interrupt_requested(job_id, should_stop):
                    stopped = True
                    break
                cls = geo.classify_point(ev["lon"], ev["lat"], target_regions)
                decision = resolve_classification(cls, rule)
                self.store.upsert_attribution(
                    ev["event_id"], job["boundary_version_to"],
                    job["rule_version"], decision["status"],
                    decision["region_id"], list(cls.region_ids),
                    decision["needs_review"],
                )
                if decision["needs_review"]:
                    self.store.open_conflict(
                        ev["event_id"], job["boundary_version_to"],
                        job["rule_version"], decision["conflict_reason"],
                        list(cls.region_ids),
                    )
                    conflict_count += 1
                self.store.mark_job_progress(job_id, ev["event_id"])
                processed += 1
                if processed % BATCH_COMMIT_EVENTS == 0:
                    self.store.conn.execute("COMMIT")
                    self.store.conn.execute(
                        "UPDATE jobs SET processed_count=?, conflict_count=? WHERE job_id=?",
                        (processed, conflict_count, job_id),
                    )
                    self.store.conn.execute("BEGIN IMMEDIATE")
        except Exception as exc:
            self.store.conn.execute("ROLLBACK")
            self.store.conn.execute(
                "UPDATE jobs SET state='FAILED', error=?, finished_at=? WHERE job_id=?",
                (repr(exc), utcnow(), job_id),
            )
            raise

        if stopped:
            self.store.conn.execute("COMMIT")
            self.store.conn.execute(
                "UPDATE jobs SET state='INTERRUPTED', processed_count=?, "
                "conflict_count=? WHERE job_id=?",
                (processed, conflict_count, job_id),
            )
            self.store.set_meta("interrupt_job_id", "")
            return {"job_id": job_id, "state": "INTERRUPTED",
                    "processed": processed}

        self.store.conn.execute("COMMIT")
        self.store.conn.execute(
            "UPDATE jobs SET state='COMPLETED', processed_count=?, "
            "conflict_count=?, finished_at=? WHERE job_id=?",
            (processed, conflict_count, utcnow(), job_id),
        )
        self.store.set_meta("interrupt_job_id", "")
        return {"job_id": job_id, "state": "COMPLETED",
                "processed": processed, "conflicts": conflict_count}

    def _interrupt_requested(self, job_id, should_stop) -> bool:
        if should_stop is not None and should_stop():
            return True
        return self.store.get_meta("interrupt_job_id") == str(job_id)

    @staticmethod
    def request_interrupt(store, job_id: int):
        store.set_meta("interrupt_job_id", str(job_id))
