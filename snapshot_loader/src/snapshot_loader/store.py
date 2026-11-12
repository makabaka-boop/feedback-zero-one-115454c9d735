"""SQLite persistence for the loader.

Every piece of progress is durable so that a restart resumes in place without
re-sampling any chunk:

* ``meta``        - bootstrap parameters, the contiguous log cursor and the
                    currently published position;
* ``chunks``      - per-chunk scan lifecycle and watermarks L/H;
* ``sample_rows`` - immutable rows returned by the single sample of a chunk;
* ``recon_rows``  - reconciled per-chunk rows after (L, H] writes are applied;
* ``log_entries`` - every consumed log entry (dedupe / corruption detection);
* ``replica``     - the published full copy of the source (only readable via
                    the query API once a publish has committed).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .protocol import ProtocolError, dec_pos, enc_pos, require_u64

# chunk lifecycle
CH_NEW = "new"
CH_SCANNING = "scanning"
CH_RECONCILED = "reconciled"
CH_FAILED = "failed"


class DuplicatePosError(Exception):
    """Same pos seen again with different content (spec: hard failure)."""


class Store:
    def __init__(self, db_path: str):
        self._lock = threading.RLock()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA busy_timeout=10000")
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        self._db.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                s TEXT,
                ranges_json TEXT,
                cursor TEXT NOT NULL DEFAULT {enc_pos(0)!r},
                published_c TEXT,
                failed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY NOT NULL,
                lo TEXT,
                hi TEXT,
                status TEXT NOT NULL DEFAULT '{CH_NEW}',
                l TEXT,
                h TEXT
            );
            CREATE TABLE IF NOT EXISTS sample_rows (
                chunk_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (chunk_id, key)
            );
            CREATE TABLE IF NOT EXISTS recon_rows (
                chunk_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (chunk_id, key)
            );
            CREATE TABLE IF NOT EXISTS log_entries (
                pos TEXT PRIMARY KEY NOT NULL,
                kind TEXT NOT NULL,
                key TEXT,
                after_json TEXT
            );
            CREATE TABLE IF NOT EXISTS replica (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT
            );
            """
        )
        self._db.commit()

    # ------------------------------------------------------------ bootstrap
    def is_bootstrapped(self) -> bool:
        with self._lock:
            row = self._db.execute("SELECT s FROM meta WHERE id = 1").fetchone()
            return row is not None and row["s"] is not None

    def save_bootstrap(self, s: int, ranges: List[Tuple[Optional[str], Optional[str]]]) -> None:
        require_u64(s, "s")
        with self._lock:
            # The log is only provided *after* s, so the contiguous cursor
            # starts at s rather than at 0.
            self._db.execute(
                "INSERT INTO meta(id, s, ranges_json, cursor) VALUES (1, ?, ?, ?) "
                "ON CONFLICT(id) DO NOTHING",
                (enc_pos(s), json.dumps([list(r) for r in ranges]), enc_pos(s)),
            )
            existing = {
                r["chunk_id"]
                for r in self._db.execute("SELECT chunk_id FROM chunks").fetchall()
            }
            for idx, (lo, hi) in enumerate(ranges):
                cid = str(idx)
                if cid not in existing:
                    self._db.execute(
                        "INSERT INTO chunks(chunk_id, lo, hi) VALUES (?, ?, ?)",
                        (cid, lo, hi),
                    )
            self._db.commit()

    def get_meta(self) -> Dict[str, Any]:
        with self._lock:
            row = self._db.execute(
                "SELECT s, ranges_json, cursor, published_c, failed FROM meta WHERE id = 1"
            ).fetchone()
            if row is None or row["s"] is None:
                raise ProtocolError("loader has not bootstrapped yet")
            return {
                "s": dec_pos(row["s"]),
                "ranges": [tuple(r) for r in json.loads(row["ranges_json"])],
                "cursor": dec_pos(row["cursor"]),
                "published_c": None if row["published_c"] is None else dec_pos(row["published_c"]),
                "failed": bool(row["failed"]),
            }

    def set_failed(self) -> None:
        with self._lock:
            self._db.execute("UPDATE meta SET failed = 1 WHERE id = 1")
            self._db.execute(
                f"UPDATE chunks SET status = '{CH_FAILED}' WHERE status IN (?, ?)",
                (CH_NEW, CH_SCANNING),
            )
            self._db.commit()

    # --------------------------------------------------------------- chunks
    def get_chunks(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._db.execute(
                "SELECT chunk_id, lo, hi, status, l, h FROM chunks ORDER BY chunk_id"
            ).fetchall()]

    def get_chunk(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._db.execute(
                "SELECT chunk_id, lo, hi, status, l, h FROM chunks WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
            return None if row is None else dict(row)

    def mark_scanning(self, chunk_id: str) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE chunks SET status = ? WHERE chunk_id = ? AND status = ?",
                (CH_SCANNING, chunk_id, CH_NEW),
            )
            self._db.commit()

    def save_scan_result(
        self,
        chunk_id: str,
        l_pos: int,
        h_pos: int,
        rows: List[Dict[str, Any]],
    ) -> None:
        """Persist the immutable sample + watermarks (idempotent on retry)."""
        require_u64(l_pos, "L")
        require_u64(h_pos, "H")
        with self._lock:
            chunk = self.get_chunk(chunk_id)
            if chunk is None:
                raise ProtocolError(f"unknown chunk {chunk_id}")
            if chunk["status"] == CH_RECONCILED:
                return  # fully processed already; nothing to do
            if chunk["h"] is not None:
                # retrying after a crash: content must be identical
                if dec_pos(chunk["h"]) != h_pos or dec_pos(chunk["l"]) != l_pos:
                    raise ProtocolError(
                        f"scan content changed on retry for chunk {chunk_id}"
                    )
                return
            self._db.execute(
                "UPDATE chunks SET status = ?, l = ?, h = ? WHERE chunk_id = ?",
                (CH_SCANNING, enc_pos(l_pos), enc_pos(h_pos), chunk_id),
            )
            for r in rows:
                self._db.execute(
                    "INSERT OR REPLACE INTO sample_rows(chunk_id, key, value) VALUES (?, ?, ?)",
                    (chunk_id, r["key"], r["value"]),
                )
            self._db.commit()

    def replace_recon(self, chunk_id: str, rows: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._db.execute("DELETE FROM recon_rows WHERE chunk_id = ?", (chunk_id,))
            self._db.executemany(
                "INSERT INTO recon_rows(chunk_id, key, value) VALUES (?, ?, ?)",
                [(chunk_id, r["key"], r["value"]) for r in rows],
            )
            self._db.execute(
                "UPDATE chunks SET status = ? WHERE chunk_id = ?",
                (CH_RECONCILED, chunk_id),
            )
            self._db.commit()

    def get_sample_rows(self, chunk_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._db.execute(
                "SELECT key, value FROM sample_rows WHERE chunk_id = ? ORDER BY key",
                (chunk_id,),
            ).fetchall()]

    def get_recon_rows(self, chunk_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._db.execute(
                "SELECT key, value FROM recon_rows WHERE chunk_id = ? ORDER BY key",
                (chunk_id,),
            ).fetchall()]

    def all_reconciled(self) -> bool:
        with self._lock:
            row = self._db.execute(
                f"SELECT COUNT(*) AS n FROM chunks WHERE status != '{CH_RECONCILED}'"
            ).fetchone()
            return row["n"] == 0

    # ------------------------------------------------------------- log stream
    def append_log_entry(self, entry: Dict[str, Any]) -> None:
        """Insert one consumed entry.

        Same pos + same content -> ignored (duplicate delivery, spec allows).
        Same pos + different content -> ``DuplicatePosError`` (hard failure).
        """
        pos = require_u64(entry["pos"], "entry.pos")
        kind = entry["kind"]
        if kind not in ("write", "mark"):
            raise ProtocolError(f"unknown log kind {kind!r}")
        key = entry.get("key")
        after_json = None if kind == "mark" else json.dumps(entry.get("after"))
        with self._lock:
            # Atomic insert: a concurrent process (e.g. an overlapping crash
            # test worker) may already hold this pos. ON CONFLICT makes the
            # write race-free; we then verify content for the duplicate rule.
            self._db.execute(
                "INSERT INTO log_entries(pos, kind, key, after_json) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(pos) DO NOTHING",
                (enc_pos(pos), kind, key, after_json),
            )
            existing = self._db.execute(
                "SELECT kind, key, after_json FROM log_entries WHERE pos = ?",
                (enc_pos(pos),),
            ).fetchone()
            if (
                existing["kind"] != kind
                or existing["key"] != key
                or existing["after_json"] != after_json
            ):
                raise DuplicatePosError(
                    f"pos {pos} re-delivered with different content: "
                    f"stored={dict(existing)} new={(kind, key, after_json)}"
                )
            self._db.commit()

    def entries_between(self, lo_exclusive: int, hi_inclusive: int) -> List[Dict[str, Any]]:
        """Entries with ``lo_exclusive < pos <= hi_inclusive`` in pos order."""
        with self._lock:
            rows = self._db.execute(
                "SELECT pos, kind, key, after_json FROM log_entries "
                "WHERE pos > ? AND pos <= ? ORDER BY pos",
                (enc_pos(lo_exclusive), enc_pos(hi_inclusive)),
            ).fetchall()
            return [self._decode(r) for r in rows]

    def entries_after(self, pos: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT pos, kind, key, after_json FROM log_entries "
                "WHERE pos > ? ORDER BY pos",
                (enc_pos(pos),),
            ).fetchall()
            return [self._decode(r) for r in rows]

    def advance_cursor(self, expected_old: int, new_pos: int) -> bool:
        """Monotonic cursor move; returns False if another thread already moved."""
        require_u64(new_pos, "new_pos")
        with self._lock:
            cur = dec_pos(
                self._db.execute("SELECT cursor FROM meta WHERE id = 1").fetchone()["cursor"]
            )
            if cur != expected_old or new_pos <= cur:
                return False
            self._db.execute(
                "UPDATE meta SET cursor = ? WHERE id = 1", (enc_pos(new_pos),)
            )
            self._db.commit()
            return True

    def _decode(self, row: sqlite3.Row) -> Dict[str, Any]:
        entry: Dict[str, Any] = {"pos": dec_pos(row["pos"]), "kind": row["kind"]}
        if row["kind"] == "write":
            entry["key"] = row["key"]
            entry["after"] = json.loads(row["after_json"])
        return entry

    # --------------------------------------------------------------- publish
    def publish(self, c: int, rows: List[Dict[str, Any]], crash_hook: Optional[Any] = None) -> None:
        """Atomically replace the replica and publish position C.

        A crash before commit leaves the previous publication intact (SQLite
        rolls the transaction back); the optional hook exists solely so the
        acceptance suite can prove that.
        """
        require_u64(c, "C")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.execute("DELETE FROM replica")
                self._db.executemany(
                    "INSERT INTO replica(key, value) VALUES (?, ?)",
                    [(r["key"], r["value"]) for r in rows],
                )
                self._db.execute(
                    "UPDATE meta SET published_c = ? WHERE id = 1", (enc_pos(c),)
                )
                if crash_hook is not None:
                    crash_hook("before_commit")  # test hook: os._exit happens here
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def get_replica(self) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        with self._lock:
            row = self._db.execute("SELECT published_c FROM meta WHERE id = 1").fetchone()
            if row is None or row["published_c"] is None:
                return None, []
            rows = [dict(r) for r in self._db.execute(
                "SELECT key, value FROM replica ORDER BY key"
            ).fetchall()]
            return dec_pos(row["published_c"]), rows

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @property
    def lock(self) -> threading.RLock:
        return self._lock


# ---------------------------------------------------------------------------
# Pure reconciliation logic (unit-testable without a running source)
# ---------------------------------------------------------------------------
def apply_after(rows: Dict[str, Any], key: str, after: Any) -> None:
    """Last-write-wins mutation of an in-memory key->value map."""
    if after is None:
        rows.pop(key, None)
    else:
        rows[key] = after["value"]


def reconcile_chunk(
    sample_rows: List[Dict[str, Any]],
    between_entries: List[Dict[str, Any]],
    range_bounds: Optional[Tuple[Optional[str], Optional[str]]] = None,
) -> List[Dict[str, Any]]:
    """Build a chunk's reconciled state.

    Start from the sampled rows, then apply, in source order, every write in
    the half-open interval ``(L, H]`` whose key belongs to this chunk's range
    ``[lo, hi)``.  Marks occupy positions but change nothing.  Writes in the
    interval targeting foreign ranges are ignored here: they belong to another
    chunk's reconciliation or to the publication replay.  Deletions remove the
    key, so a key written after L and deleted before H leaves no trace even if
    the sample saw it.
    """
    from .protocol import key_in_range

    merged: Dict[str, Any] = {r["key"]: r["value"] for r in sample_rows}
    for entry in between_entries:
        if entry["kind"] != "write":
            continue
        if range_bounds is not None and not key_in_range(
            entry["key"], range_bounds[0], range_bounds[1]
        ):
            continue
        apply_after(merged, entry["key"], entry["after"])
    return [{"key": k, "value": merged[k]} for k in sorted(merged)]
