"""HTTP API + single-page console, stdlib only (http.server).

Endpoints (JSON unless noted):
  GET  /                      -> static console HTML
  GET  /api/status            -> versions, jobs, conflicts, counts
  POST /api/events            -> ingest one event {event_id, lon, lat, payload?}
  POST /api/boundaries        -> upload a GeoJSON FeatureCollection (DRAFT)
  POST /api/boundaries/<v>/job   -> create recompute job {scope?}
  POST /api/jobs/<id>/interrupt  -> request a cooperative interrupt
  POST /api/jobs/<id>/run        -> run/resume a job synchronously (worker loop)
  POST /api/boundaries/<v>/activate
  GET  /api/conflicts?state=...
  POST /api/conflicts/<id>/resolve  {region_id}
  POST /api/rules             -> propose {on_boundary, overlap, rationale?}
  POST /api/rules/<id>/approve
  POST /api/reports           -> {boundary_version, rule_version?, compare_version?,
                                  seq_from?, seq_to?}
  GET  /api/reports
  POST /api/reports/<id>/reproduce
  GET  /api/events?limit=...

The server runs the worker loop inline when /run is called; interruption is
cooperative (polled per event), so a huge job stops at a clean transaction
boundary and resumes later without reprocessing events.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from pointwise import app as services
from pointwise.geometry import GeometryError
from pointwise.storage import DuplicateEvent, Store
from pointwise.worker import Worker

STATIC_DIR = Path(__file__).parent / "static"


class _Handler(BaseHTTPRequestHandler):
    server_version = "pointwise/1.0"

    # -- plumbing ----------------------------------------------------------
    def log_message(self, fmt, *args):  # quieter, single-line access log
        self.server.log(f"{self.address_string()} {fmt % args}")

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, content_type: str):
        if not path.is_file():
            self._send_json({"error": f"missing static file {path.name}"}, 404)
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise _HttpError(400, f"invalid JSON body: {exc}")

    def _store(self) -> Store:
        return self.server.store

    # -- routing -----------------------------------------------------------
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/":
                return self._send_file(STATIC_DIR / "index.html", "text/html; charset=utf-8")
            path = path.rstrip("/")
            qs = parse_qs(parsed.query)
            if path == "/api/status":
                return self._send_json(services.system_status(self._store()))
            if path == "/api/conflicts":
                state = (qs.get("state") or ["OPEN"])[0]
                bv = qs.get("boundary_version", [None])[0]
                rows = self._store().list_conflicts(
                    state=state or None,
                    boundary_version=int(bv) if bv else None,
                )
                return self._send_json([dict(r) for r in rows])
            if path == "/api/events":
                limit = int((qs.get("limit") or ["200"])[0])
                return self._send_json(
                    [dict(r) for r in self._store().list_events(limit=limit)]
                )
            if path == "/api/reports":
                return self._send_json(
                    [dict(r) for r in self._store().list_reports()]
                )
            raise _HttpError(404, f"no route for GET {path}")
        except _HttpError as e:
            self._send_json({"error": e.message}, e.status)
        except Exception as e:  # noqa: BLE001 - surface as JSON, keep server up
            self._send_json({"error": repr(e)}, 500)

    def do_POST(self):
        try:
            path = urlparse(self.path).path.rstrip("/")
            body = self._read_json()
            store = self._store()

            if path == "/api/events":
                try:
                    result = services.ingest_event(
                        store, body["event_id"], float(body["lon"]),
                        float(body["lat"]), body.get("payload"),
                    )
                except (KeyError, TypeError) as exc:
                    raise _HttpError(400, f"event needs event_id, lon, lat: {exc}")
                except DuplicateEvent as exc:
                    raise _HttpError(409, f"duplicate event: {exc}")
                except GeometryError as exc:
                    raise _HttpError(400, str(exc))
                return self._send_json(result, 201)

            if path == "/api/boundaries":
                try:
                    result = services.upload_boundary(
                        store, body, source_name=body.get("source_name")
                    )
                except (ValueError, GeometryError) as exc:
                    raise _HttpError(400, str(exc))
                return self._send_json(result, 201)

            if (seg := _match(path, "/api/boundaries", "/job")):
                return self._send_json(
                    services.create_recompute_job(
                        store, int(seg[0]), scope=body.get("scope"),
                        rule_version=body.get("rule_version"),
                    ), 201
                )

            if (seg := _match(path, "/api/boundaries", "/activate")):
                try:
                    return self._send_json(
                        services.activate_version(store, int(seg[0]))
                    )
                except RuntimeError as exc:
                    raise _HttpError(409, str(exc))

            if (seg := _match(path, "/api/jobs", "/interrupt")):
                try:
                    return self._send_json(
                        services.interrupt_job(store, int(seg[0]))
                    )
                except RuntimeError as exc:
                    raise _HttpError(409, str(exc))

            if (seg := _match(path, "/api/jobs", "/run")):
                return self._run_job(int(seg[0]))

            if (seg := _match(path, "/api/conflicts", "/resolve")):
                try:
                    return self._send_json(services.resolve_conflict(
                        store, int(seg[0]), body["region_id"],
                        body.get("decided_by", "web-operator"),
                    ))
                except (KeyError, ValueError) as exc:
                    raise _HttpError(400, str(exc))
                except RuntimeError as exc:
                    raise _HttpError(409, str(exc))

            if path == "/api/rules":
                try:
                    return self._send_json(services.propose_rule(
                        store, body["on_boundary"], body["overlap"],
                        body.get("rationale"),
                    ), 201)
                except ValueError as exc:
                    raise _HttpError(400, str(exc))

            if (seg := _match(path, "/api/rules", "/approve")):
                try:
                    return self._send_json(
                        services.approve_rule(store, int(seg[0]))
                    )
                except RuntimeError as exc:
                    raise _HttpError(409, str(exc))

            if path == "/api/reports":
                from pointwise.reports import run_report
                try:
                    return self._send_json(run_report(
                        store,
                        boundary_version=int(body["boundary_version"]),
                        rule_version=body.get("rule_version"),
                        seq_from=int(body.get("seq_from", 1)),
                        seq_to=body.get("seq_to"),
                        compare_version=body.get("compare_version"),
                    ), 201)
                except (KeyError, TypeError, ValueError) as exc:
                    raise _HttpError(400, str(exc))

            if (seg := _match(path, "/api/reports", "/reproduce")):
                from pointwise.reports import reproduce_report
                try:
                    return self._send_json(reproduce_report(store, int(seg[0])))
                except (ValueError, RuntimeError) as exc:
                    raise _HttpError(409, str(exc))

            raise _HttpError(404, f"no route for POST {path}")
        except _HttpError as e:
            self._send_json({"error": e.message}, e.status)
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": repr(e)}, 500)

    def _run_job(self, job_id: int):
        store = self._store()
        job = store.get_job(job_id)
        if job is None:
            raise _HttpError(404, f"unknown job {job_id}")
        # One worker loop at a time; interruption remains available because
        # the loop polls shared meta state per event.
        with self.server.job_lock:
            result = Worker(store).run_job(job_id)
        return self._send_json(result)


def _match(path: str, prefix: str, suffix: str):
    """Match /prefix/<int>/suffix and return (id,)."""
    parts = path.strip("/").split("/")
    expected = prefix.strip("/").split("/") + ["id", suffix.strip("/")]
    if len(parts) != len(expected):
        return None
    for p, e in zip(parts, expected):
        if e == "id":
            if not p.isdigit():
                return None
        elif p != e:
            return None
    return (int(parts[len(prefix.strip("/").split("/"))]),)


class _HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class PointwiseServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, db_path: str, log=print):
        super().__init__(addr, _Handler)
        self.store = Store(db_path)
        services.ensure_defaults(self.store)
        self.job_lock = threading.Lock()
        self._log = log

    def log(self, msg: str):
        self._log(msg)

    def server_close(self):
        try:
            self.store.close()
        finally:
            super().server_close()


def serve(host: str = "127.0.0.1", port: int = 8080,
          db_path: str = "data/pointwise.db", log=print) -> PointwiseServer:
    httpd = PointwiseServer((host, port), db_path, log=log)
    log(f"pointwise web on http://{host}:{port} (db={db_path})")
    return httpd
