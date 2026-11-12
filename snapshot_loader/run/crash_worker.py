"""Subprocess worker for crash-recovery acceptance tests.

Runs a loader against an already-created mock source, optionally arming a hard
``os._exit`` crash point, and prints a single JSON line describing the outcome.
All real work happens over HTTP; the only argument is a JSON config on argv.
"""

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

from snapshot_loader.app import build_loader_server  # noqa: E402
from snapshot_loader.store import Store  # noqa: E402
from tests.harness import (  # noqa: E402
    loader_advance,
    loader_query,
    loader_status,
    src_info,
    wait_all_reconciled,
    wait_cursor,
)


def main() -> int:
    cfg = json.loads(sys.argv[1])
    workdir = cfg["workdir"]
    loader_db = os.path.join(workdir, "loader.db")
    source_url = cfg["source_url"]
    port = cfg["port"]
    crash_hook = cfg.get("crash_hook") or ""
    target_c = cfg.get("target_c")

    store = Store(loader_db)
    httpd = build_loader_server(
        loader_db, source_url, "127.0.0.1", port,
        crash_hook=crash_hook, auto_scan=True,
    )
    import threading

    t = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05},
                         daemon=True)
    t.start()
    base = f"http://127.0.0.1:{port}"

    def _emit(payload):
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()

    try:
        if crash_hook in ("after_scan_started", "after_scan_received"):
            # The engine's own hook calls os._exit at the chosen point.
            # Block here until the process is killed; a normal return means
            # the hook never fired (a test failure the caller detects).
            import time

            time.sleep(10)
            _emit({"event": "survived", "status": loader_status(base)})
            return 0

        wait_all_reconciled(base)
        c = target_c if target_c is not None else src_info(source_url)["tip"]
        wait_cursor(base, c)
        result = loader_advance(base, c)
        if result[1].get("status") == "not_ready":
            # tip moved under us; re-read and retry once
            c = src_info(source_url)["tip"]
            wait_cursor(base, c)
            result = loader_advance(base, c)
        _emit({"event": "advance", "http": result[0], "body": result[1]})
        if result[1].get("status") in ("published", "already_published"):
            status, query = loader_query(base)
            _emit({"event": "query", "http": status, "body": query})
        return 0
    finally:
        try:
            httpd.engine.stop()
            httpd.shutdown()
            httpd.server_close()
            store.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
