"""Command-line entry points so each runtime side can run independently.

Examples
--------
  python -m pointwise serve --db data/pw.db --port 8080          # web/report side
  python -m pointwise worker --db data/pw.db --loop              # attribution side
  python -m pointwise ingest --db data/pw.db --event evt-1 116.4 39.9  # ingestion side
  python -m pointwise boundary upload --db data/pw.db fictional.json
  python -m pointwise job create  --db data/pw.db --to-version 2
  python -m pointwise job run     --db data/pw.db --job 1
  python -m pointwise job interrupt --db data/pw.db --job 1
  python -m pointwise activate --db data/pw.db --version 2
  python -m pointwise report --db data/pw.db --boundary 2 --compare 1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from . import app as services
from .geometry import GeometryError
from .reports import reproduce_report, run_report
from .storage import DuplicateEvent, Store
from .worker import Worker


def _store(args) -> Store:
    return Store(args.db)


def cmd_status(args):
    store = _store(args)
    print(json.dumps(services.system_status(store), ensure_ascii=False, indent=2))


def cmd_ingest(args):
    """Ingestion process entry point: one event or a JSON/JSONL batch file."""
    store = _store(args)
    if args.batch:
        path = Path(args.batch)
        ok = 0
        for i, line in enumerate(_read_batch(path)):
            eid = line.get("event_id") or f"batch-{path.stem}-{i}"
            services.ingest_event(
                store, eid, float(line["lon"]), float(line["lat"]),
                line.get("payload"),
            )
            ok += 1
        print(json.dumps({"ingested": ok}, ensure_ascii=False))
        return
    try:
        result = services.ingest_event(
            store, args.event, float(args.lon), float(args.lat),
            json.loads(args.payload) if args.payload else None,
        )
    except (DuplicateEvent, GeometryError, ValueError) as exc:
        _fail(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _read_batch(path: Path):
    text = path.read_text(encoding="utf-8")
    try:
        doc = json.loads(text)
        if isinstance(doc, list):
            yield from doc
        elif isinstance(doc, dict) and isinstance(doc.get("events"), list):
            yield from doc["events"]
        else:
            raise ValueError("batch JSON must be a list or {events:[...]}")
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)


def cmd_worker(args):
    """Attribution process: live backfill loop and/or run a recompute job."""
    store = _store(args)
    worker = Worker(store)
    if args.job is not None:
        result = worker.run_job(args.job)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.once:
        print(json.dumps(worker.attribute_pending_live_events(),
                         ensure_ascii=False, indent=2))
        return
    # Long-running side: drain live events, and pick up PENDING/INTERRUPTED
    # jobs. An interrupt flag (set by another process) pauses a job cleanly;
    # the next loop iteration resumes it from job_progress checkpoints.
    print(f"worker loop started, poll interval {args.interval}s", file=sys.stderr)
    while True:
        info = worker.attribute_pending_live_events()
        if info.get("attributed"):
            print(json.dumps({"live": info}, ensure_ascii=False), file=sys.stderr)
        row = store.conn.execute(
            "SELECT job_id FROM jobs WHERE state IN ('PENDING','INTERRUPTED') "
            "ORDER BY job_id LIMIT 1"
        ).fetchone()
        if row:
            print(json.dumps({"running_job": row["job_id"]}), file=sys.stderr)
            worker.run_job(row["job_id"])
        if args.interval <= 0:
            break
        time.sleep(args.interval)


def cmd_boundary(args):
    store = _store(args)
    if args.action == "upload":
        doc = json.loads(Path(args.file).read_text(encoding="utf-8"))
        try:
            result = services.upload_boundary(
                store, doc, source_name=Path(args.file).name
            )
        except (ValueError, GeometryError) as exc:
            _fail(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.action == "list":
        print(json.dumps([dict(b) for b in store.list_boundary_sets()],
                         ensure_ascii=False, indent=2))


def cmd_job(args):
    store = _store(args)
    if args.action == "create":
        try:
            result = services.create_recompute_job(
                store, args.to_version, scope=args.scope
            )
        except ValueError as exc:
            _fail(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.action == "list":
        print(json.dumps([dict(j) for j in store.list_jobs()],
                         ensure_ascii=False, indent=2))
    elif args.action == "interrupt":
        try:
            print(json.dumps(services.interrupt_job(store, args.job),
                             ensure_ascii=False, indent=2))
        except (ValueError, RuntimeError) as exc:
            _fail(str(exc))
    elif args.action == "run":
        print(json.dumps(Worker(store).run_job(args.job),
                         ensure_ascii=False, indent=2))


def cmd_activate(args):
    store = _store(args)
    try:
        print(json.dumps(services.activate_version(store, args.version),
                         ensure_ascii=False, indent=2))
    except RuntimeError as exc:
        _fail(str(exc))


def cmd_rule(args):
    store = _store(args)
    if args.action == "propose":
        try:
            result = services.propose_rule(
                store, args.on_boundary, args.overlap, args.rationale
            )
        except ValueError as exc:
            _fail(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.action == "approve":
        try:
            print(json.dumps(services.approve_rule(store, args.rule_version),
                             ensure_ascii=False, indent=2))
        except RuntimeError as exc:
            _fail(str(exc))
    elif args.action == "list":
        print(json.dumps([dict(r) for r in store.list_rules()],
                         ensure_ascii=False, indent=2))


def cmd_conflict(args):
    store = _store(args)
    if args.action == "list":
        rows = store.list_conflicts(state=args.state or "OPEN")
        print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))
    elif args.action == "resolve":
        try:
            print(json.dumps(services.resolve_conflict(
                store, args.conflict_id, args.region_id, "cli-operator"),
                ensure_ascii=False, indent=2))
        except (ValueError, RuntimeError) as exc:
            _fail(str(exc))


def cmd_report(args):
    store = _store(args)
    if args.reproduce is not None:
        print(json.dumps(reproduce_report(store, args.reproduce),
                         ensure_ascii=False, indent=2))
        return
    try:
        print(json.dumps(run_report(
            store, boundary_version=args.boundary,
            rule_version=args.rule_version,
            seq_from=args.seq_from, seq_to=args.seq_to,
            compare_version=args.compare,
        ), ensure_ascii=False, indent=2))
    except ValueError as exc:
        _fail(str(exc))


def cmd_serve(args):
    from .web.server import serve

    httpd = serve(args.host, args.port, args.db)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def cmd_demo(args):
    """Populate a small, fully deterministic demo dataset."""
    from .seed_demo import seed_demo

    store = _store(args)
    print(json.dumps(seed_demo(store), ensure_ascii=False, indent=2))


def _fail(msg: str):
    print(json.dumps({"error": msg}, ensure_ascii=False), file=sys.stderr)
    sys.exit(2)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pointwise", description=__doc__)
    p.add_argument("--db", default="data/pointwise.db", help="SQLite database path")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="show full system status").set_defaults(
        func=cmd_status)

    sp = sub.add_parser("serve", help="run web/API + report side")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    sp.set_defaults(func=cmd_serve)

    sp = sub.add_parser("ingest", help="ingestion side: accept event(s)")
    sp.add_argument("event", nargs="?", help="event id")
    sp.add_argument("lon", nargs="?", type=float)
    sp.add_argument("lat", nargs="?", type=float)
    sp.add_argument("--payload", help="JSON payload string")
    sp.add_argument("--batch", help="JSON list / {events:[...]} / JSONL file")
    sp.set_defaults(func=cmd_ingest)

    sp = sub.add_parser("worker", help="attribution side: classify + recompute")
    sp.add_argument("--once", action="store_true", help="one live-attribution pass")
    sp.add_argument("--job", type=int, help="run/resume a specific recompute job")
    sp.add_argument("--interval", type=float, default=2.0,
                    help="loop poll seconds (0 = single pass and exit)")
    sp.set_defaults(func=cmd_worker)

    sp = sub.add_parser("boundary", help="upload/list boundary sets")
    bsp = sp.add_subparsers(dest="action", required=True)
    up = bsp.add_parser("upload")
    up.add_argument("file")
    bsp.add_parser("list")
    sp.set_defaults(func=cmd_boundary)

    sp = sub.add_parser("job", help="recompute jobs")
    jsp = sp.add_subparsers(dest="action", required=True)
    cr = jsp.add_parser("create")
    cr.add_argument("--to-version", type=int, required=True)
    cr.add_argument("--scope", choices=["FULL", "CHANGED_REGIONS"])
    jsp.add_parser("list")
    ir = jsp.add_parser("interrupt")
    ir.add_argument("--job", type=int, required=True)
    rn = jsp.add_parser("run")
    rn.add_argument("--job", type=int, required=True)
    sp.set_defaults(func=cmd_job)

    sp = sub.add_parser("activate", help="activate a completed boundary version")
    sp.add_argument("--version", type=int, required=True)
    sp.set_defaults(func=cmd_activate)

    sp = sub.add_parser("rule", help="propose/approve/list attribution rules")
    rsp = sp.add_subparsers(dest="action", required=True)
    pr = rsp.add_parser("propose")
    pr.add_argument("--on-boundary", required=True, choices=["MIN_ID", "MANUAL"])
    pr.add_argument("--overlap", required=True, choices=["MIN_ID", "MANUAL"])
    pr.add_argument("--rationale")
    ap = rsp.add_parser("approve")
    ap.add_argument("--rule-version", type=int, required=True)
    rsp.add_parser("list")
    sp.set_defaults(func=cmd_rule)

    sp = sub.add_parser("conflict", help="inspect/resolve boundary/overlap conflicts")
    csp = sp.add_subparsers(dest="action", required=True)
    ls = csp.add_parser("list")
    ls.add_argument("--state", default="OPEN")
    rs = csp.add_parser("resolve")
    rs.add_argument("--conflict-id", type=int, required=True)
    rs.add_argument("--region-id", required=True)
    sp.set_defaults(func=cmd_conflict)

    sp = sub.add_parser("report", help="run or reproduce an aggregated report")
    sp.add_argument("--boundary", type=int)
    sp.add_argument("--rule-version", type=int)
    sp.add_argument("--compare", type=int, help="compare against this boundary v")
    sp.add_argument("--seq-from", type=int, default=1)
    sp.add_argument("--seq-to", type=int)
    sp.add_argument("--reproduce", type=int, help="report id to re-run and verify")
    sp.set_defaults(func=cmd_report)

    sub.add_parser("demo", help="seed a deterministic demo dataset").set_defaults(
        func=cmd_demo)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
