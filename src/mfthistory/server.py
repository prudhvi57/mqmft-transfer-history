"""A small HTTP server: JSON search API plus the self service web page.

Standard library only, so it runs anywhere Python runs. Put it behind your normal
reverse proxy (TLS, SSO) for production use.

GET /                         self service page
GET /healthz                  liveness
GET /api/me                   who the token belongs to and which agents it can see
GET /api/transfers            search: transfer_id, agent, file, status, job, since, until, limit, offset
GET /api/transfers/<id>       one transfer with its files and message timeline
GET /api/transfers.csv        same filters as search, as CSV (for audits)
GET /api/stuck?minutes=60     transfers that started and never finished
GET /api/stats?since=...      counts by status, by day and by source agent
GET /api/agents               agents visible to the caller
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from .auth import TokenRegistry
from .store import HistoryStore, Scope

log = logging.getLogger("mfthistory.server")

SEARCH_KEYS = ("transfer_id", "agent", "file", "status", "job", "since", "until", "limit", "offset")
CSV_COLUMNS = ("transfer_id", "started_at", "completed_at", "status", "result_code", "source_agent",
               "destination_agent", "job_name", "file_count", "total_bytes", "supplement")


def make_handler(store: HistoryStore, registry: TokenRegistry):
    lock = threading.Lock()
    page = resources.files("mfthistory").joinpath("web/index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        server_version = "mfthistory"

        def log_message(self, fmt, *args):  # route to logging instead of stderr
            log.info("%s %s", self.address_string(), fmt % args)

        def _send(self, code: int, body: bytes, ctype: str = "application/json", extra: Optional[dict] = None):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy",
                             "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, obj) -> None:
            self._send(code, json.dumps(obj, default=str).encode())

        def _scope(self) -> Optional[Scope]:
            auth = self.headers.get("Authorization", "")
            token = auth[7:].strip() if auth.lower().startswith("bearer ") else None
            return registry.scope_for(token)

        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            path = url.path.rstrip("/") or "/"
            if path == "/":
                return self._send(200, page, "text/html; charset=utf-8")
            if path == "/healthz":
                return self._json(200, {"ok": True})
            if not path.startswith("/api/"):
                return self._json(404, {"error": "not found"})

            scope = self._scope()
            if scope is None:
                return self._json(401, {"error": "missing or unknown token"})
            q = {k: v[0] for k, v in parse_qs(url.query).items() if v}
            try:
                with lock:
                    return self._route(path, q, scope)
            except (ValueError, TypeError) as exc:
                return self._json(400, {"error": str(exc)})

        def _route(self, path: str, q: dict, scope: Scope):
            if path == "/api/me":
                return self._json(200, {"name": scope.name,
                                        "agents": "*" if scope.agents is None else sorted(scope.agents)})
            if path == "/api/transfers":
                args = {k: q[k] for k in SEARCH_KEYS if k in q}
                return self._json(200, store.search(scope=scope, **args))
            if path == "/api/transfers.csv":
                args = {k: q[k] for k in SEARCH_KEYS if k in q}
                args["limit"] = 500
                rows = store.search(scope=scope, **args)["results"]
                buf = io.StringIO()
                w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)
                return self._send(200, buf.getvalue().encode(), "text/csv; charset=utf-8",
                                  {"Content-Disposition": "attachment; filename=transfers.csv"})
            if path.startswith("/api/transfers/"):
                tid = unquote(path[len("/api/transfers/"):])
                row = store.get(tid, scope=scope)
                if row is None:
                    return self._json(404, {"error": "transfer not found"})
                return self._json(200, row)
            if path == "/api/stuck":
                return self._json(200, {"results": store.stuck(float(q.get("minutes", 60)), scope=scope)})
            if path == "/api/stats":
                return self._json(200, store.stats(scope=scope, since=q.get("since")))
            if path == "/api/agents":
                return self._json(200, {"agents": store.agents(scope=scope)})
            return self._json(404, {"error": "not found"})

    return Handler


def serve(store: HistoryStore, registry: TokenRegistry, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), make_handler(store, registry))
    log.info("listening on http://%s:%s", host, httpd.server_address[1])
    return httpd
