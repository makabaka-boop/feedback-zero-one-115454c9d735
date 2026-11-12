"""HTTP facade for the loader and the command-line entry points.

Loader API
----------
``GET  /query``          -> ``{C, rows}`` once published, else ``not_ready``
``POST /advance``  body ``{"C": <u64>}`` -> publication result
``GET  /status``         -> debugging view of cursor/chunks/publication
``POST /shutdown``      -> graceful stop (test convenience)
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict

from .loader import LoaderEngine
from .protocol import ProtocolError, RemoteError
from .store import DuplicatePosError, Store
from .source import serve as serve_source


class _LoaderHandler(BaseHTTPRequestHandler):
    server_version = "SnapshotLoader/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        engine: LoaderEngine = self.server.engine  # type: ignore[attr-defined]
        path = self.path.split("?", 1)[0]
        try:
            if path == "/query":
                result = engine.query()
                self._send(200 if result.get("status") != "not_ready" else 409, result)
            elif path == "/status":
                self._send(200, engine.status())
            else:
                self._send(404, {"error": f"no route for GET {path}"})
        except (ProtocolError, RemoteError) as exc:
            self._send(400, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        engine: LoaderEngine = self.server.engine  # type: ignore[attr-defined]
        path = self.path.split("?", 1)[0]
        try:
            body = self._read_json()
            if path == "/advance":
                result = engine.advance(body["C"])
                status = 200
                if result["status"] == "rejected":
                    status = 409
                elif result["status"] == "failed":
                    status = 500
                self._send(status, result)
            elif path == "/shutdown":
                engine.stop()
                self._send(200, {"status": "stopping"})
            else:
                self._send(404, {"error": f"no route for POST {path}"})
        except DuplicatePosError as exc:
            self._send(500, {"status": "failed", "error": str(exc)})
        except (KeyError, TypeError, ProtocolError, json.JSONDecodeError) as exc:
            self._send(400, {"error": str(exc)})


def build_loader_server(
    db_path: str, source_url: str, host: str, port: int,
    crash_hook: str = "", verbose: bool = False, auto_scan: bool = True,
) -> ThreadingHTTPServer:
    store = Store(db_path)
    engine = LoaderEngine(
        store, source_url, crash_hook=crash_hook or None, auto_scan=auto_scan
    )
    httpd = ThreadingHTTPServer((host, port), _LoaderHandler)
    httpd.engine = engine  # type: ignore[attr-defined]
    httpd.verbose = verbose  # type: ignore[attr-defined]
    return httpd


def main(argv: Any = None) -> int:
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Incremental snapshot loader / mock source")
    sub = parser.add_subparsers(dest="cmd", required=True)

    src = sub.add_parser("source", help="run the mock HTTP source")
    src.add_argument("--db", required=True)
    src.add_argument("--host", default="127.0.0.1")
    src.add_argument("--port", type=int, required=True)
    src.add_argument("--chunks", type=int, default=2, choices=(1, 2))
    src.add_argument("--seed", help="JSON object of key -> {'value': ...} or null-after")
    src.add_argument(
        "--manual",
        action="store_true",
        help="gate scan phase transitions behind /test/scan-control (tests)",
    )
    src.add_argument("--verbose", action="store_true")

    ld = sub.add_parser("loader", help="run the loader")
    ld.add_argument("--db", required=True)
    ld.add_argument("--source", required=True, help="base URL of the mock source")
    ld.add_argument("--host", default="127.0.0.1")
    ld.add_argument("--port", type=int, required=True)
    ld.add_argument(
        "--crash-hook",
        default="",
        choices=(
            "",
            "after_scan_started",
            "after_scan_received",
            "before_publish",
            "in_publish_before_commit",
            "after_publish_commit",
        ),
        help="hard-exit at an internal point (acceptance tests)",
    )
    ld.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)
    if args.cmd == "source":
        seed = json.loads(args.seed) if args.seed else None
        httpd = serve_source(
            args.db, args.host, args.port, args.chunks, seed=seed,
            verbose=args.verbose, manual=args.manual,
        )
    else:
        httpd = build_loader_server(
            args.db, args.source, args.host, args.port,
            crash_hook=args.crash_hook, verbose=args.verbose,
        )

    def _handle(signum: int, frame: Any) -> None:
        # Raising in the main thread unwinds serve_forever() and runs the
        # finally cleanup. Calling httpd.shutdown() here would deadlock
        # because serve_forever() owns this same thread.
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    print(f"listening on {httpd.server_address[0]}:{httpd.server_address[1]}", flush=True)
    try:
        httpd.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass  # SIGTERM/SIGINT: graceful termination requested
    finally:
        engine = getattr(httpd, "engine", None)
        if engine is not None and hasattr(engine, "stop"):
            engine.stop()
        if engine is not None and hasattr(engine, "close"):
            engine.close()
        httpd.server_close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
