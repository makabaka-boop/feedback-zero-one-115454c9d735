"""Shared harness for in-process and subprocess acceptance tests.

Everything here speaks HTTP to the source/loader exactly as a real deployment
would; the only in-process shortcut is starting the two ``ThreadingHTTPServer``
instances on ephemeral ports.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional

# Make src importable when this file is used from a standalone worker script.
_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "src")
for _p in (_SRC, os.path.dirname(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from snapshot_loader.app import build_loader_server  # noqa: E402
from snapshot_loader.protocol import RemoteError, http_json  # noqa: E402
from snapshot_loader.source import SCAN_DONE, serve as serve_source  # noqa: E402
from snapshot_loader.store import Store  # noqa: E402


# --------------------------------------------------------------------- HTTP
def request(method: str, base: str, path: str, body: Any = None,
            query: Optional[Dict[str, Any]] = None, timeout: float = 10.0) -> Any:
    url = base + path
    if query:
        from urllib.parse import urlencode

        url += "?" + urlencode(query)
    # production client: retries transport/connection-level failures
    return http_json(method, url, body, timeout=timeout)


def request_status(method: str, base: str, path: str, body: Any = None,
                   query: Optional[Dict[str, Any]] = None) -> tuple:
    """Like request() but returns (status_code, json_body)."""
    import urllib.error

    url = base + path
    if query:
        from urllib.parse import urlencode

        url += "?" + urlencode(query)
    try:
        return 200, http_json(method, url, body)
    except RemoteError as exc:
        return exc.status, json.loads(exc.body or "{}")
    except urllib.error.HTTPError as exc:  # defensive: kept for completeness
        return exc.code, json.loads(exc.read().decode("utf-8"))


# ------------------------------------------------------------- source verbs
def src_write(base: str, key: str, after: Any) -> int:
    return request("POST", base, "/test/write", {"key": key, "after": after})["pos"]


def src_mark(base: str) -> int:
    return request("POST", base, "/test/mark", {})["pos"]


def src_snapshot(base: str, c: int) -> Dict[str, Any]:
    return request("GET", base, "/test/snapshot", query={"C": c})


def src_info(base: str) -> Dict[str, Any]:
    return request("GET", base, "/test/info")


def scan_start(base: str, chunk_id: str) -> Dict[str, Any]:
    return request("POST", base, "/scan", {"chunk_id": chunk_id})


def scan_continue(base: str, chunk_id: str) -> Dict[str, Any]:
    return request("POST", base, "/scan/continue", {"chunk_id": chunk_id})


def scan_control(base: str, chunk_id: str, phase: str) -> Dict[str, Any]:
    wrapped = request(
        "POST", base, "/test/scan-control", {"chunk_id": chunk_id, "phase": phase}
    )
    return wrapped["after"]


def drive_manual_chunk(base: str, chunk_id: str) -> Dict[str, Any]:
    """Drive a gated chunk scan to completion; return its final done payload."""
    started = scan_start(base, chunk_id)
    if started["status"] == SCAN_DONE:
        return started
    sampled = scan_control(base, chunk_id, "sampled")
    assert sampled["status"] == "sampled", sampled
    done = scan_control(base, chunk_id, "done")
    assert done["status"] == SCAN_DONE, done
    return done


# ------------------------------------------------------------- loader verbs
def loader_advance(base: str, c: int) -> tuple:
    return request_status("POST", base, "/advance", {"C": c})


def loader_query(base: str) -> tuple:
    return request_status("GET", base, "/query")


def loader_status(base: str) -> Dict[str, Any]:
    return request("GET", base, "/status")


def wait_until(predicate, timeout: float = 15.0, interval: float = 0.05,
               label: str = "condition") -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {label}; last={last!r}")


def wait_cursor(loader_base: str, target: int, timeout: float = 15.0) -> None:
    wait_until(
        lambda: loader_status(loader_base)["cursor"] >= target,
        timeout=timeout, label=f"cursor >= {target}",
    )


def wait_chunk_status(loader_base: str, chunk_id: str, status: str) -> None:
    wait_until(
        lambda: any(
            c["chunk_id"] == chunk_id and c["status"] == status
            for c in loader_status(loader_base)["chunks"]
        ),
        timeout=15.0, label=f"chunk {chunk_id} == {status}",
    )


def wait_all_reconciled(loader_base: str) -> None:
    wait_until(
        lambda: all(
            c["status"] == "reconciled"
            for c in loader_status(loader_base)["chunks"]
        ),
        timeout=15.0, label="all chunks reconciled",
    )


def wait_published(loader_base: str) -> dict:
    def _published():
        status, body = loader_query(loader_base)
        return body if status == 200 else None

    return wait_until(_published, timeout=15.0, label="first publication")


def advance_until(loader_base: str, c: int, acceptable=("published", "already_published")) -> dict:
    """Poll advance(C) until it leaves the transient not_ready state.

    A ``chunks_pending`` / ``log_not_consumed_through_C`` answer is transient;
    a ``C_below_chunk_H`` answer means C was chosen too small and is a test bug.
    """

    def _try():
        status, body = loader_advance(loader_base, c)
        if body.get("status") in acceptable:
            return body
        if body.get("status") == "not_ready" and body.get("reason") in (
            "chunks_pending",
            "log_not_consumed_through_C",
        ):
            return None
        raise AssertionError(f"unexpected advance response {status} {body}")

    return wait_until(_try, timeout=15.0, label=f"advance({c}) publication")


# ----------------------------------------------------------------- harness
class Harness:
    """Starts source + loader servers on ephemeral ports in daemon threads."""

    def __init__(self, seed: Optional[Dict[str, Any]] = None, *, manual: bool = False,
                 crash_hook: Optional[str] = None, workdir: Optional[str] = None,
                 num_chunks: int = 2, auto_scan: bool = True,
                 start_loader: bool = True):
        self.tmp = tempfile.TemporaryDirectory(dir=workdir)
        self.source_db = os.path.join(self.tmp.name, "source.db")
        self.loader_db = os.path.join(self.tmp.name, "loader.db")
        self._servers: List[Any] = []
        self._threads: List[threading.Thread] = []

        self.source_server = serve_source(
            self.source_db, "127.0.0.1", 0, num_chunks, seed=seed, manual=manual
        )
        self._start(self.source_server, "source")
        self.source_url = f"http://127.0.0.1:{self.source_server.server_address[1]}"

        self.store = None
        self.loader_server = None
        if start_loader:
            self.store = Store(self.loader_db)
            self.loader_server = build_loader_server(
                self.loader_db, self.source_url, "127.0.0.1", 0,
                crash_hook=crash_hook or "", auto_scan=auto_scan,
            )
            self._start(self.loader_server, "loader")
            self.loader_url = f"http://127.0.0.1:{self.loader_server.server_address[1]}"
        else:
            self.loader_url = None

    def _start(self, server: Any, name: str) -> None:
        t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05},
                             name=f"http-{name}", daemon=True)
        t.start()
        self._threads.append(t)
        self._servers.append(server)

    def stop(self) -> None:
        if self.loader_server is not None:
            engine = getattr(self.loader_server, "engine", None)
            if engine is not None:
                engine.stop()
        for server in self._servers:
            server.shutdown()
            server.server_close()
        if self.store is not None:
            self.store.close()
        self.tmp.cleanup()


def assert_snapshot_equal(testcase: Any, actual: Dict[str, Any], expected: Dict[str, Any],
                          msg: str = "") -> None:
    testcase.assertEqual(actual["C"], expected["C"], msg)
    testcase.assertEqual(
        [(r["key"], r["value"]) for r in actual["rows"]],
        [(r["key"], r["value"]) for r in expected["rows"]],
        msg,
    )
