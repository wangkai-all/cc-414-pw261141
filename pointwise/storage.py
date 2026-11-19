"""SQLite-backed storage and versioning state machine.

The three runtime processes (ingest / worker / web) share one database file.
Writers use BEGIN IMMEDIATE transactions so ingestion, attribution and
recompute jobs serialise safely; readers run in WAL mode and never block.

Versioning invariants
---------------------
* events are append-only and immutable (raw lon/lat kept forever);
* an attribution row is keyed by (event_id, boundary_version, rule_version),
  so old- and new-boundary results for the *same* event coexist;
* a recompute job pins snapshot_seq at creation and only touches events with
  seq <= snapshot_seq, therefore events arriving mid-job can never be counted
  under two boundary versions inside one job;
* a DRAFT boundary cannot become ACTIVE while its recompute job is unfinished.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS boundary_sets (
  version        INTEGER PRIMARY KEY AUTOINCREMENT,
  state          TEXT NOT NULL CHECK (state IN
                   ('DRAFT','ACTIVE','SUPERSEDED','REJECTED')),
  source_name    TEXT,
  geometry_hash  TEXT NOT NULL,
  region_count   INTEGER NOT NULL,
  self_intersects INTEGER NOT NULL DEFAULT 0,
  validation_notes TEXT NOT NULL DEFAULT '[]',
  created_at     TEXT NOT NULL,
  activated_at   TEXT,
  superseded_by  INTEGER
);

CREATE TABLE IF NOT EXISTS regions (
  version       INTEGER NOT NULL REFERENCES boundary_sets(version),
  region_id     TEXT NOT NULL,
  name          TEXT,
  geometry_json TEXT NOT NULL,
  bbox_json     TEXT NOT NULL,
  geom_hash     TEXT NOT NULL,
  PRIMARY KEY (version, region_id)
);

CREATE TABLE IF NOT EXISTS rules (
  rule_version INTEGER PRIMARY KEY AUTOINCREMENT,
  state        TEXT NOT NULL CHECK (state IN ('PENDING','APPROVED','REJECTED')),
  on_boundary  TEXT NOT NULL,
  overlap      TEXT NOT NULL,
  unlocated    TEXT NOT NULL DEFAULT 'UNLOCATED',
  rationale    TEXT,
  created_at   TEXT NOT NULL,
  approved_at  TEXT
);

CREATE TABLE IF NOT EXISTS events (
  event_id     TEXT PRIMARY KEY,
  seq          INTEGER NOT NULL UNIQUE,
  received_at  TEXT NOT NULL,
  lon          REAL NOT NULL,
  lat          REAL NOT NULL,
  payload_json TEXT
);

CREATE TABLE IF NOT EXISTS attributions (
  event_id         TEXT NOT NULL,
  boundary_version INTEGER NOT NULL,
  rule_version     INTEGER NOT NULL,
  status           TEXT NOT NULL,
  region_id        TEXT,
  candidate_ids    TEXT NOT NULL DEFAULT '[]',
  needs_review     INTEGER NOT NULL DEFAULT 0,
  computed_at      TEXT NOT NULL,
  PRIMARY KEY (event_id, boundary_version, rule_version)
);

CREATE TABLE IF NOT EXISTS conflicts (
  conflict_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id         TEXT NOT NULL,
  boundary_version INTEGER NOT NULL,
  rule_version     INTEGER NOT NULL,
  reason           TEXT NOT NULL,
  candidate_ids    TEXT NOT NULL,
  chosen_region_id TEXT,
  state            TEXT NOT NULL CHECK (state IN ('OPEN','RESOLVED','IGNORED')),
  created_at       TEXT NOT NULL,
  resolved_at      TEXT,
  decided_by       TEXT,
  UNIQUE (event_id, boundary_version, rule_version)
);

CREATE TABLE IF NOT EXISTS jobs (
  job_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  job_type               TEXT NOT NULL DEFAULT 'RECOMPUTE',
  boundary_version_from  INTEGER,
  boundary_version_to    INTEGER NOT NULL,
  rule_version           INTEGER NOT NULL,
  scope                  TEXT NOT NULL CHECK (scope IN ('FULL','CHANGED_REGIONS')),
  impact_json            TEXT NOT NULL,
  state                  TEXT NOT NULL CHECK (state IN
                           ('PENDING','RUNNING','INTERRUPTED','COMPLETED','FAILED')),
  snapshot_seq           INTEGER NOT NULL,
  processed_count        INTEGER NOT NULL DEFAULT 0,
  conflict_count         INTEGER NOT NULL DEFAULT 0,
  error                  TEXT,
  created_at             TEXT NOT NULL,
  started_at             TEXT,
  finished_at            TEXT
);

CREATE TABLE IF NOT EXISTS job_progress (
  job_id   INTEGER NOT NULL REFERENCES jobs(job_id),
  event_id TEXT NOT NULL,
  PRIMARY KEY (job_id, event_id)
);

CREATE TABLE IF NOT EXISTS report_runs (
  report_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  boundary_version INTEGER NOT NULL,
  rule_version     INTEGER NOT NULL,
  compare_version  INTEGER,
  seq_from         INTEGER NOT NULL,
  seq_to           INTEGER NOT NULL,
  params_hash      TEXT NOT NULL,
  totals_json      TEXT NOT NULL,
  comparison_json  TEXT,
  created_at       TEXT NOT NULL
);

-- Hot-path indexes: versioned attribution lookups, conflict queues and
-- job progress scans stay index-backed as the event table grows.
CREATE INDEX IF NOT EXISTS idx_attributions_versions
  ON attributions(boundary_version, rule_version);
CREATE INDEX IF NOT EXISTS idx_conflicts_state
  ON conflicts(state, boundary_version);
CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(SCHEMA)

    def close(self):
        self.conn.close()

    @contextmanager
    def tx(self):
        """Serializable-ish write transaction (single SQLite writer)."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    # -- meta --------------------------------------------------------------
    def get_meta(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def get_active_versions(self) -> dict:
        return {
            "boundary_version": self._int_or_none(self.get_meta("active_boundary_version")),
            "rule_version": self._int_or_none(self.get_meta("active_rule_version")),
        }

    @staticmethod
    def _int_or_none(v):
        return int(v) if v is not None else None

    # -- boundary sets -----------------------------------------------------
    def insert_boundary_set(
        self, source_name, geometry_hash, region_count, self_intersects, notes
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO boundary_sets(state,source_name,geometry_hash,region_count,"
            "self_intersects,validation_notes,created_at) VALUES('DRAFT',?,?,?,?,?,?)",
            (source_name, geometry_hash, region_count, int(self_intersects),
             json.dumps(notes), utcnow()),
        )
        return cur.lastrowid

    def insert_region(self, version, region_id, name, geometry, bbox, geom_hash):
        self.conn.execute(
            "INSERT INTO regions(version,region_id,name,geometry_json,bbox_json,geom_hash)"
            " VALUES(?,?,?,?,?,?)",
            (version, region_id, name, json.dumps(geometry), json.dumps(bbox), geom_hash),
        )

    def get_boundary_set(self, version: int):
        return self.conn.execute(
            "SELECT * FROM boundary_sets WHERE version=?", (version,)
        ).fetchone()

    def list_boundary_sets(self):
        return self.conn.execute(
            "SELECT * FROM boundary_sets ORDER BY version"
        ).fetchall()

    def list_regions(self, version: int):
        return self.conn.execute(
            "SELECT region_id,name,geometry_json,bbox_json,geom_hash "
            "FROM regions WHERE version=? ORDER BY region_id",
            (version,),
        ).fetchall()

    def activate_boundary_set(self, version: int) -> bool:
        """Atomically promote a DRAFT/version to ACTIVE and supersede the old.

        Fails (returns False) while a recompute job for this version is still
        unfinished: mixing versions mid-recompute is not permitted.
        """
        with self.tx() as c:
            bs = c.execute(
                "SELECT * FROM boundary_sets WHERE version=?", (version,)
            ).fetchone()
            if bs is None or bs["state"] in ("REJECTED", "ACTIVE"):
                return False
            open_job = c.execute(
                "SELECT 1 FROM jobs WHERE boundary_version_to=? "
                "AND state IN ('PENDING','RUNNING','INTERRUPTED')",
                (version,),
            ).fetchone()
            if open_job:
                return False
            old = self.get_meta("active_boundary_version")
            c.execute(
                "UPDATE boundary_sets SET state='ACTIVE', activated_at=? WHERE version=?",
                (utcnow(), version),
            )
            if old is not None:
                c.execute(
                    "UPDATE boundary_sets SET state='SUPERSEDED', superseded_by=? "
                    "WHERE version=?",
                    (version, int(old)),
                )
            c.execute(
                "INSERT INTO meta(key,value) VALUES('active_boundary_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(version),),
            )
        return True

    # -- rules -------------------------------------------------------------
    def insert_rule(self, on_boundary, overlap, rationale=None,
                    state="PENDING", unlocated="UNLOCATED") -> int:
        cur = self.conn.execute(
            "INSERT INTO rules(state,on_boundary,overlap,unlocated,rationale,created_at)"
            " VALUES(?,?,?,?,?,?)",
            (state, on_boundary, overlap, unlocated, rationale, utcnow()),
        )
        return cur.lastrowid

    def get_rule(self, rule_version: int):
        return self.conn.execute(
            "SELECT * FROM rules WHERE rule_version=?", (rule_version,)
        ).fetchone()

    def list_rules(self):
        return self.conn.execute("SELECT * FROM rules ORDER BY rule_version").fetchall()

    def approve_rule(self, rule_version: int) -> bool:
        with self.tx() as c:
            row = c.execute(
                "SELECT state FROM rules WHERE rule_version=?", (rule_version,)
            ).fetchone()
            if row is None or row["state"] != "PENDING":
                return False
            c.execute(
                "UPDATE rules SET state='APPROVED', approved_at=? WHERE rule_version=?",
                (utcnow(), rule_version),
            )
            c.execute(
                "INSERT INTO meta(key,value) VALUES('active_rule_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(rule_version),
            ))
        return True

    # -- events ------------------------------------------------------------
    def insert_event(self, event_id: str, lon: float, lat: float, payload=None) -> int:
        """Append one event; returns its monotonic seq. Fails on duplicate id."""
        with self.tx() as c:
            exists = c.execute(
                "SELECT 1 FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if exists:
                raise DuplicateEvent(event_id)
            nxt = int(self.get_meta("event_seq", "0")) + 1
            c.execute(
                "INSERT INTO events(event_id,seq,received_at,lon,lat,payload_json)"
                " VALUES(?,?,?,?,?,?)",
                (event_id, nxt, utcnow(), lon, lat,
                 json.dumps(payload) if payload is not None else None),
            )
            c.execute(
                "INSERT INTO meta(key,value) VALUES('event_seq',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(nxt),
            ))
        return nxt

    def get_event(self, event_id: str):
        return self.conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()

    def list_events(self, seq_from=None, seq_to=None, limit=None):
        q = "SELECT * FROM events WHERE 1=1"
        args = []
        if seq_from is not None:
            q += " AND seq>=?"
            args.append(seq_from)
        if seq_to is not None:
            q += " AND seq<=?"
            args.append(seq_to)
        q += " ORDER BY seq"
        if limit:
            q += f" LIMIT {int(limit)}"
        return self.conn.execute(q, args).fetchall()

    def max_seq(self) -> int:
        return int(self.get_meta("event_seq", "0"))

    # -- attributions / conflicts ------------------------------------------
    def upsert_attribution(self, event_id, boundary_version, rule_version,
                           status, region_id, candidate_ids, needs_review):
        self.conn.execute(
            "INSERT INTO attributions(event_id,boundary_version,rule_version,"
            "status,region_id,candidate_ids,needs_review,computed_at) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(event_id,boundary_version,rule_version) DO UPDATE SET "
            "status=excluded.status, region_id=excluded.region_id, "
            "candidate_ids=excluded.candidate_ids, needs_review=excluded.needs_review, "
            "computed_at=excluded.computed_at",
            (event_id, boundary_version, rule_version, status, region_id,
             json.dumps(candidate_ids), int(needs_review), utcnow()),
        )

    def get_attribution(self, event_id, boundary_version, rule_version):
        return self.conn.execute(
            "SELECT * FROM attributions WHERE event_id=? AND boundary_version=? "
            "AND rule_version=?",
            (event_id, boundary_version, rule_version),
        ).fetchone()

    def open_conflict(self, event_id, boundary_version, rule_version,
                      reason, candidate_ids):
        self.conn.execute(
            "INSERT INTO conflicts(event_id,boundary_version,rule_version,reason,"
            "candidate_ids,state,created_at) VALUES(?,?,?,?,?,'OPEN',?) "
            "ON CONFLICT(event_id,boundary_version,rule_version) DO UPDATE SET "
            "state='OPEN', chosen_region_id=NULL, resolved_at=NULL",
            (event_id, boundary_version, rule_version, reason,
             json.dumps(candidate_ids), utcnow()),
        )

    def list_conflicts(self, state="OPEN", boundary_version=None):
        q = ("SELECT c.*, e.lon, e.lat, e.seq FROM conflicts c "
             "JOIN events e ON e.event_id=c.event_id WHERE 1=1")
        args = []
        if state:
            q += " AND c.state=?"
            args.append(state)
        if boundary_version is not None:
            q += " AND c.boundary_version=?"
            args.append(boundary_version)
        q += " ORDER BY c.conflict_id"
        return self.conn.execute(q, args).fetchall()

    def resolve_conflict(self, conflict_id: int, chosen_region_id: str,
                         decided_by: str = "operator") -> bool:
        with self.tx() as c:
            row = c.execute(
                "SELECT * FROM conflicts WHERE conflict_id=?", (conflict_id,)
            ).fetchone()
            if row is None or row["state"] != "OPEN":
                return False
            candidates = json.loads(row["candidate_ids"])
            if chosen_region_id not in candidates:
                raise ValueError(
                    f"region {chosen_region_id!r} is not a candidate "
                    f"({candidates}) for conflict {conflict_id}"
                )
            c.execute(
                "UPDATE conflicts SET state='RESOLVED', chosen_region_id=?,"
                "resolved_at=?,decided_by=? WHERE conflict_id=?",
                (chosen_region_id, utcnow(), decided_by, conflict_id),
            )
            c.execute(
                "UPDATE attributions SET needs_review=0, region_id=?, status='RESOLVED' "
                "WHERE event_id=? AND boundary_version=? AND rule_version=?",
                (chosen_region_id, row["event_id"], row["boundary_version"],
                 row["rule_version"]),
            )
        return True

    # -- jobs --------------------------------------------------------------
    def create_job(self, boundary_version_from, boundary_version_to, rule_version,
                   scope, impact, snapshot_seq) -> int:
        cur = self.conn.execute(
            "INSERT INTO jobs(job_type,boundary_version_from,boundary_version_to,"
            "rule_version,scope,impact_json,state,snapshot_seq,created_at) "
            "VALUES('RECOMPUTE',?,?,?,?,?,'PENDING',?,?)",
            (boundary_version_from, boundary_version_to, rule_version, scope,
             json.dumps(impact), snapshot_seq, utcnow()),
        )
        return cur.lastrowid

    def get_job(self, job_id: int):
        return self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()

    def list_jobs(self):
        return self.conn.execute("SELECT * FROM jobs ORDER BY job_id DESC").fetchall()

    def mark_job_progress(self, job_id, event_id):
        self.conn.execute(
            "INSERT OR IGNORE INTO job_progress(job_id,event_id) VALUES(?,?)",
            (job_id, event_id),
        )

    def job_done_events(self, job_id) -> set:
        return {
            r["event_id"]
            for r in self.conn.execute(
                "SELECT event_id FROM job_progress WHERE job_id=?", (job_id,)
            )
        }

    # -- reports -----------------------------------------------------------
    def insert_report(self, boundary_version, rule_version, compare_version,
                      seq_from, seq_to, params_hash, totals, comparison) -> int:
        cur = self.conn.execute(
            "INSERT INTO report_runs(boundary_version,rule_version,compare_version,"
            "seq_from,seq_to,params_hash,totals_json,comparison_json,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (boundary_version, rule_version, compare_version, seq_from, seq_to,
             params_hash, json.dumps(totals),
             json.dumps(comparison) if comparison is not None else None, utcnow()),
        )
        return cur.lastrowid

    def list_reports(self):
        return self.conn.execute(
            "SELECT * FROM report_runs ORDER BY report_id DESC"
        ).fetchall()


class DuplicateEvent(Exception):
    pass
