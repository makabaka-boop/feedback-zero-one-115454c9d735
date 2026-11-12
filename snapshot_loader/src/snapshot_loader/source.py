"""Local HTTP mock source backed by SQLite.

The source models the contract from the specification:

* a key/value table whose values are non-null ASCII keys mapped to strings or
  SQL NULL (a real null value is distinct from a deleted key);
* every write produces a strictly increasing, gap-free positive ``pos`` and a
  complete ``after`` object (``null`` after = deletion);
* watermarks are ordinary log entries that also occupy a ``pos``;
* ``GET /bootstrap`` returns the log start position ``s`` and a fixed set of
  ascending, non-overlapping, half-open key ranges covering every key
  (``null`` endpoint means infinity);
* each chunk scan is performed against ONE atomic sample of the source state.
  The low mark L is written before sampling and the high mark H after, all in
  source order, with client writes allowed in between.  The same sample is
  returned unchanged on every retry of the chunk;
* scans may be delayed arbitrarily: the two-phase protocol lets a test park a
  scan between L and the sample, and between the sample and H, while writes
  continue concurrently;
* ``GET /log`` serves the write/mark stream from ``after_pos``; gaps simply do
  not exist (the stream is contiguous), blocking-style waiting is emulated by
  returning what is available so the loader can poll.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

from .protocol import (
    ProtocolError,
    dec_pos,
    enc_pos,
    require_u64,
    validate_after,
    validate_key,
)

# scan phases used both on the wire and inside the engine
SCAN_REQUESTED = "requested"   # L may now be written; waiting on /scan/continue
SCAN_SAMPLED = "sampled"       # sample taken; waiting on /scan/continue -> H
SCAN_DONE = "done"             # H written; rows are available forever


class SourceEngine:
    """All stateful source logic; an RLock serializes every SQLite access."""

    def __init__(self, db_path: str, num_chunks: int = 2, seed: Optional[Dict[str, Any]] = None,
                 manual: bool = False):
        self._lock = threading.RLock()
        # Manual mode: /scan/continue never advances a phase on its own; the
        # test control endpoint is the only way forward.  This lets a test park
        # a scan deterministically between L and the sample (and between the
        # sample and H) while concurrent writes land in between.
        self._manual = manual
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA busy_timeout=10000")
        self._init_schema()
        self._num_chunks = int(num_chunks)
        with self._lock:
            if self._db.execute("SELECT COUNT(*) FROM meta").fetchone()[0] == 0:
                self._bootstrap(seed or {})
        row = self._db.execute("SELECT s, ranges_json FROM meta").fetchone()
        self._s = dec_pos(row["s"])
        self._ranges: List[Tuple[Optional[str], Optional[str]]] = [
            tuple(r) for r in json.loads(row["ranges_json"])
        ]

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                s TEXT NOT NULL,
                ranges_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS log (
                pos TEXT PRIMARY KEY NOT NULL,
                kind TEXT NOT NULL,
                key TEXT,
                after_json TEXT
            );
            CREATE TABLE IF NOT EXISTS scans (
                chunk_id TEXT PRIMARY KEY NOT NULL,
                phase TEXT NOT NULL,
                l TEXT,
                h TEXT,
                payload_json TEXT
            );
            """
        )
        self._db.commit()

    def _bootstrap(self, seed: Dict[str, Any]) -> None:
        pos = 0
        for key in sorted(seed):  # deterministic initial order
            validate_key(key)
            after = validate_after(seed[key])
            pos += 1
            self._db.execute(
                "INSERT INTO kv(key, value) VALUES (?, ?)",
                (key, None if after is None else after["value"]),
            )
            self._db.execute(
                "INSERT INTO log(pos, kind, key, after_json) VALUES (?, 'write', ?, ?)",
                (enc_pos(pos), key, json.dumps(after)),
            )
        # s = position after which the log is served to the loader; the seed
        # history occupies (0, s] and is never exposed through /log.
        s = pos
        self._db.execute(
            "INSERT INTO meta(id, s, ranges_json) VALUES (1, ?, ?)",
            (enc_pos(s), json.dumps(self._initial_ranges())),
        )
        self._db.commit()

    def _initial_ranges(self) -> List[Tuple[Optional[str], Optional[str]]]:
        """Fixed split point 'B': everything below B in chunk 0, rest in 1."""
        if self._num_chunks <= 1:
            return [(None, None)]
        if self._num_chunks != 2:
            raise ValueError("mock source supports 1 or 2 chunks")
        return [(None, "B"), ("B", None)]

    # ------------------------------------------------------------- append log
    def _append(self, kind: str, key: Optional[str], after: Any) -> int:
        """Append one log entry. Caller holds the lock; returns new pos."""
        row = self._db.execute("SELECT MAX(pos) AS m FROM log").fetchone()
        pos = (dec_pos(row["m"]) if row["m"] is not None else 0) + 1
        self._db.execute(
            "INSERT INTO log(pos, kind, key, after_json) VALUES (?, ?, ?, ?)",
            (enc_pos(pos), kind, key, None if after is _MARK else json.dumps(after)),
        )
        return pos

    def write(self, key: str, after: Any) -> int:
        validate_key(key)
        validate_after(after)
        with self._lock:
            pos = self._append("write", key, after)
            if after is None:
                self._db.execute("DELETE FROM kv WHERE key = ?", (key,))
            else:
                self._db.execute(
                    "INSERT INTO kv(key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, after["value"]),
                )
            self._db.commit()
            return pos

    def mark(self) -> int:
        with self._lock:
            pos = self._append("mark", None, _MARK)
            self._db.commit()
            return pos

    # ---------------------------------------------------------------- endpoints
    def bootstrap(self) -> Dict[str, Any]:
        return {
            "s": self._s,
            "ranges": [
                {"chunk_id": str(i), "lo": lo, "hi": hi}
                for i, (lo, hi) in enumerate(self._ranges)
            ],
        }

    def scan_start(self, chunk_id: str) -> Dict[str, Any]:
        cid = self._check_chunk(chunk_id)
        with self._lock:
            row = self._db.execute(
                "SELECT phase, l, h, payload_json FROM scans WHERE chunk_id = ?",
                (cid,),
            ).fetchone()
            if row is not None:
                if row["phase"] == SCAN_DONE:
                    return self._scan_response(cid, row)
                # parked in an earlier phase: retries see the same L
                return {
                    "status": row["phase"],
                    "chunk_id": cid,
                    "L": dec_pos(row["l"]),
                }
            # L: low watermark, strictly before the sample.
            l_pos = self._append("mark", None, _MARK)
            self._db.execute(
                "INSERT INTO scans(chunk_id, phase, l) VALUES (?, ?, ?)",
                (cid, SCAN_REQUESTED, enc_pos(l_pos)),
            )
            self._db.commit()
            return {"status": SCAN_REQUESTED, "chunk_id": cid, "L": l_pos}

    def scan_continue(self, chunk_id: str, *, _force: bool = False) -> Dict[str, Any]:
        cid = self._check_chunk(chunk_id)
        with self._lock:
            row = self._db.execute(
                "SELECT phase, l, h, payload_json FROM scans WHERE chunk_id = ?",
                (cid,),
            ).fetchone()
            if row is None:
                raise ProtocolError(f"scan for chunk {cid} was never started")
            phase = row["phase"]
            if self._manual and not _force and phase != SCAN_DONE:
                # gated: phase transitions only happen via /test/scan-control;
                # a finished scan always returns its immutable result
                return {"status": phase, "chunk_id": cid}
            if phase == SCAN_REQUESTED:
                # One atomic sample of the whole kv table at this instant.
                sample = [
                    {"key": r["key"], "value": r["value"]}
                    for r in self._db.execute(
                        "SELECT key, value FROM kv ORDER BY key"
                    ).fetchall()
                ]
                lo, hi = self._ranges[int(cid)]
                payload = [r for r in sample if _in(r["key"], lo, hi)]
                self._db.execute(
                    "UPDATE scans SET phase = ?, payload_json = ? WHERE chunk_id = ?",
                    (SCAN_SAMPLED, json.dumps(payload), cid),
                )
                self._db.commit()
                return {"status": SCAN_SAMPLED, "chunk_id": cid}
            if phase == SCAN_SAMPLED:
                # H: high watermark, strictly after the sample.
                h_pos = self._append("mark", None, _MARK)
                self._db.execute(
                    "UPDATE scans SET phase = ?, h = ? WHERE chunk_id = ?",
                    (SCAN_DONE, enc_pos(h_pos), cid),
                )
                self._db.commit()
                row = self._db.execute(
                    "SELECT phase, l, h, payload_json FROM scans WHERE chunk_id = ?",
                    (cid,),
                ).fetchone()
                return self._scan_response(cid, row)
            # already done: retries return the identical, immutable result
            return self._scan_response(cid, row)

    def _scan_response(self, cid: str, row: sqlite3.Row) -> Dict[str, Any]:
        if row["phase"] != SCAN_DONE:
            return {"status": row["phase"], "chunk_id": cid}
        return {
            "status": SCAN_DONE,
            "chunk_id": cid,
            "L": dec_pos(row["l"]),
            "H": dec_pos(row["h"]),
            "rows": json.loads(row["payload_json"]),
        }

    def scan_control(self, chunk_id: str, target_phase: str) -> Dict[str, Any]:
        """Test helper: force a parked scan through its remaining phases."""
        cid = self._check_chunk(chunk_id)
        if target_phase not in (SCAN_SAMPLED, SCAN_DONE):
            raise ProtocolError("target_phase must be 'sampled' or 'done'")
        with self._lock:
            row = self._db.execute(
                "SELECT phase FROM scans WHERE chunk_id = ?", (cid,)
            ).fetchone()
            if row is None:
                raise ProtocolError(f"scan for chunk {cid} was never started")
            result: Dict[str, Any] = {"chunk_id": cid, "from": row["phase"]}
            if row["phase"] == SCAN_DONE:
                result["after"] = self.scan_continue(cid)
            elif (row["phase"], target_phase) in (
                (SCAN_REQUESTED, SCAN_SAMPLED),
                (SCAN_SAMPLED, SCAN_DONE),
            ):
                result["after"] = self.scan_continue(cid, _force=True)
            else:
                # already at or beyond the requested target: report state
                result["after"] = self._scan_response(
                    cid,
                    self._db.execute(
                        "SELECT phase, l, h, payload_json FROM scans WHERE chunk_id = ?",
                        (cid,),
                    ).fetchone(),
                )
            return result

    def log(self, after_pos: int, limit: int) -> Dict[str, Any]:
        require_u64(after_pos, "after_pos")
        if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= 10000):
            raise ProtocolError("limit must be an int in [1, 10000]")
        with self._lock:
            # The stream is only served after the bootstrap start s; asking
            # for an older position simply yields the same post-s tail.
            effective = max(after_pos, self._s)
            entries = [
                self._decode_log_row(r)
                for r in self._db.execute(
                    "SELECT pos, kind, key, after_json FROM log "
                    "WHERE pos > ? ORDER BY pos LIMIT ?",
                    (enc_pos(effective), limit),
                ).fetchall()
            ]
        return {"entries": entries}

    def _decode_log_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        entry: Dict[str, Any] = {"pos": dec_pos(row["pos"]), "kind": row["kind"]}
        if row["kind"] == "write":
            entry["key"] = row["key"]
            entry["after"] = json.loads(row["after_json"])
        return entry

    def snapshot_at(self, c: int) -> Dict[str, Any]:
        """Debug oracle: exact source copy as of log position C."""
        require_u64(c, "C")
        with self._lock:
            max_row = self._db.execute("SELECT MAX(pos) AS m FROM log").fetchone()
            max_pos = dec_pos(max_row["m"]) if max_row["m"] is not None else 0
            if c > max_pos:
                raise ProtocolError(f"C={c} is beyond the current tip {max_pos}")
            rows: List[Dict[str, Any]] = []
            if c == 0:
                return {"C": c, "rows": rows}
            # last-write-wins per key over (0, C]; deletions remove the key.
            for r in self._db.execute(
                "SELECT key, after_json FROM log WHERE kind = 'write' AND pos <= ? "
                "ORDER BY pos",
                (enc_pos(c),),
            ).fetchall():
                after = json.loads(r["after_json"])
                if after is None:
                    rows = [x for x in rows if x["key"] != r["key"]]
                else:
                    found = False
                    for x in rows:
                        if x["key"] == r["key"]:
                            x["value"] = after["value"]
                            found = True
                            break
                    if not found:
                        rows.append({"key": r["key"], "value": after["value"]})
            rows.sort(key=lambda x: x["key"])
            return {"C": c, "rows": rows}

    def info(self) -> Dict[str, Any]:
        with self._lock:
            tip_row = self._db.execute("SELECT MAX(pos) AS m FROM log").fetchone()
            tip = dec_pos(tip_row["m"]) if tip_row["m"] is not None else 0
            scans = [
                {"chunk_id": r["chunk_id"], "phase": r["phase"]}
                for r in self._db.execute("SELECT chunk_id, phase FROM scans ORDER BY chunk_id")
            ]
        return {"s": self._s, "tip": tip, "ranges": self.bootstrap()["ranges"], "scans": scans}

    def _check_chunk(self, chunk_id: str) -> str:
        if not isinstance(chunk_id, str) or not chunk_id.isdigit():
            raise ProtocolError("chunk_id must be a decimal string")
        cid = int(chunk_id)
        if not (0 <= cid < len(self._ranges)):
            raise ProtocolError(f"unknown chunk_id {chunk_id}")
        return str(cid)

    def close(self) -> None:
        with self._lock:
            self._db.close()


class _MarkSentinel:
    pass


_MARK = _MarkSentinel()


def _in(key: str, lo: Optional[str], hi: Optional[str]) -> bool:
    return (lo is None or key >= lo) and (hi is None or key < hi)


# --------------------------------------------------------------------- HTTP
class _Handler(BaseHTTPRequestHandler):
    server_version = "MockSource/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _handle(self, method: str) -> None:
        engine: SourceEngine = self.server.engine  # type: ignore[attr-defined]
        try:
            path = self.path.split("?", 1)[0]
            body = self._read_json() if method == "POST" else {}
            if method == "GET" and path == "/bootstrap":
                self._send(200, engine.bootstrap())
            elif method == "POST" and path == "/scan":
                self._send(200, engine.scan_start(str(body["chunk_id"])))
            elif method == "POST" and path == "/scan/continue":
                self._send(200, engine.scan_continue(str(body["chunk_id"])))
            elif method == "POST" and path == "/test/scan-control":
                self._send(200, engine.scan_control(str(body["chunk_id"]), str(body["phase"])))
            elif method == "GET" and path == "/log":
                qs = self._query()
                self._send(
                    200,
                    engine.log(int(qs.get("after_pos", "0")), int(qs.get("limit", "1000"))),
                )
            elif method == "POST" and path == "/test/write":
                self._send(200, {"pos": engine.write(str(body["key"]), body.get("after"))})
            elif method == "POST" and path == "/test/mark":
                self._send(200, {"pos": engine.mark()})
            elif method == "GET" and path == "/test/snapshot":
                self._send(200, engine.snapshot_at(int(self._query()["C"])))
            elif method == "GET" and path == "/test/info":
                self._send(200, engine.info())
            else:
                self._send(404, {"error": f"no route for {method} {path}"})
        except (KeyError, TypeError, ProtocolError, json.JSONDecodeError, ValueError) as exc:
            self._send(400, {"error": str(exc)})

    def _query(self) -> Dict[str, str]:
        from urllib.parse import parse_qs, urlsplit

        q = parse_qs(urlsplit(self.path).query)
        return {k: v[-1] for k, v in q.items()}

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")


def serve(db_path: str, host: str, port: int, num_chunks: int,
          seed: Optional[Dict[str, Any]] = None, verbose: bool = False,
          manual: bool = False) -> ThreadingHTTPServer:
    engine = SourceEngine(db_path, num_chunks=num_chunks, seed=seed, manual=manual)
    httpd = ThreadingHTTPServer((host, port), _Handler)
    httpd.engine = engine  # type: ignore[attr-defined]
    httpd.verbose = verbose  # type: ignore[attr-defined]
    return httpd
