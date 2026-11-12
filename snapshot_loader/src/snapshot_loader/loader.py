"""Loader engine: chunk scans, (L, H] reconciliation and atomic publication.

Algorithm (watermark-based incremental snapshot)
------------------------------------------------
1. ``bootstrap`` once: remember the log start ``s`` and the fixed chunk
   ranges.  Every chunk begins in ``new``.
2. One scanner thread per chunk drives the two-phase source scan.  The source
   writes low mark L, then takes its single atomic sample, then writes high
   mark H; writes may be interleaved around the sample and the loader is not
   told where inside (L, H] the sample fell.  The immutable sample plus L/H is
   persisted before any further work, so retries and restarts never resample.
3. Reconciliation of a chunk: take the sampled rows and apply, in source
   order, every consumed write with ``L < pos <= H`` (marks are no-ops).
   Because every write touching the chunk's keys in (L, H] ends in the log,
   the result equals the source state at H for that key range regardless of
   where the sample was taken:
     * keys touched in (L, H] get their last write in the interval;
     * every other key keeps its sampled value (it changed nowhere in (L,H]).
4. A single log consumer tail-reads the contiguous stream: gaps make it wait,
   duplicate positions with identical content are ignored, differing content
   flips the loader into a permanent failed state while the last published
   replica stays readable.
5. ``advance(C)`` publishes only when every chunk is reconciled with
   ``H <= C`` and the log has been consumed contiguously through C.  The
   replica at C is built from the per-chunk reconciled states (each valid at
   its own H) replayed with all consumed writes in ``(max H, C]``; marker
   positions count toward contiguity.  Replica replacement + publication of C
   commit in one SQLite transaction, so a crash either fully commits or fully
   leaves the previous publication in place.
"""

from __future__ import annotations

import http.client
import os
import threading
import time
from typing import Any, Dict, List, Optional

from .protocol import (
    RemoteError,
    find_chunk,
    http_json,
    require_u64,
    validate_after,
    validate_key,
)
from .source import SCAN_DONE
from .store import (
    CH_NEW,
    CH_RECONCILED,
    CH_SCANNING,
    DuplicatePosError,
    Store,
    reconcile_chunk,
)

LOG_BATCH = 1000
LOG_POLL_SECONDS = 0.05


class NotReadyError(Exception):
    """Raised when advance(C) cannot publish yet."""


class LoaderEngine:
    def __init__(
        self,
        store: Store,
        source_url: str,
        *,
        crash_hook: Optional[str] = None,
        auto_scan: bool = True,
    ):
        self.store = store
        self.source_url = source_url.rstrip("/")
        self._crash_hook_name = crash_hook
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._wake = threading.Event()
        self._boot_lock = threading.Lock()
        if not store.is_bootstrapped():
            self._bootstrap()
        if auto_scan:
            self.start()

    # ------------------------------------------------------------- lifecycle
    def _bootstrap(self) -> None:
        with self._boot_lock:
            if self.store.is_bootstrapped():
                return
            payload = http_json("GET", f"{self.source_url}/bootstrap")
            s = require_u64(payload["s"], "s")
            ranges = []
            for r in payload["ranges"]:
                lo, hi = r["lo"], r["hi"]
                if lo is not None:
                    validate_key(lo)
                if hi is not None:
                    validate_key(hi)
                ranges.append((lo, hi))
            # ascending, non-overlapping, full coverage sanity check
            for (lo1, hi1), (lo2, hi2) in zip(ranges, ranges[1:]):
                if hi1 != lo2:
                    raise ValueError(f"ranges are not contiguous half-open: {ranges}")
            self.store.save_bootstrap(s, ranges)

    def start(self) -> None:
        if self._threads:
            return
        meta = self.store.get_meta()
        if meta["failed"]:
            return  # hard-failed loaders do no further background work
        consumer = threading.Thread(
            target=self._log_loop, name="log-consumer", daemon=True
        )
        consumer.start()
        self._threads.append(consumer)
        for chunk in self.store.get_chunks():
            if chunk["status"] in (CH_NEW, CH_SCANNING):
                t = threading.Thread(
                    target=self._scan_loop,
                    args=(chunk["chunk_id"],),
                    name=f"scan-{chunk['chunk_id']}",
                    daemon=True,
                )
                t.start()
                self._threads.append(t)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def close(self) -> None:
        self.stop()
        self.store.close()

    # ----------------------------------------------------------- crash hook
    def _crash_hook(self, point: str) -> None:
        """Configurable hard-exit points for the acceptance suite."""
        if self._crash_hook_name == point:
            # flush what we can, then die without running finally blocks
            try:
                os._exit(86)
            finally:  # pragma: no cover
                os._exit(86)

    # ---------------------------------------------------------- log consumer
    def _log_loop(self) -> None:
        while not self._stop.is_set():
            try:
                meta = self.store.get_meta()
                if meta["failed"]:
                    return
                cursor = meta["cursor"]
                payload = http_json(
                    "GET",
                    f"{self.source_url}/log?after_pos={cursor}&limit={LOG_BATCH}",
                    timeout=15,
                )
                entries = payload.get("entries", [])
                if not entries:
                    self._wake.wait(LOG_POLL_SECONDS)
                    self._wake.clear()
                    continue
                self._consume_entries(entries, expected_after=cursor)
                self._wake.set()  # scanners/advancers may be waiting on the log
            except DuplicatePosError:
                self.store.set_failed()
                return
            except (RemoteError, http.client.HTTPException,
                    KeyError, ValueError, OSError) as exc:
                # transient source/transport trouble: back off and retry
                if self._stop.wait(0.25):
                    return
                continue

    def _consume_entries(self, entries: List[Dict[str, Any]], expected_after: int) -> None:
        """Validate contiguity and persist a batch of log entries.

        Entries at or below the current cursor are duplicates: identical
        content is ignored (and tracked within this batch), differing content
        raises via the store.  A future position while an earlier one is
        missing means a gap: stop without moving the cursor and re-poll.
        """
        expected = expected_after
        seen: set = set()
        for entry in entries:
            pos = require_u64(entry["pos"], "entry.pos")
            if entry["kind"] == "write":
                validate_key(entry["key"])
                validate_after(entry["after"])
            elif entry["kind"] != "mark":
                raise ValueError(f"unknown log kind {entry.get('kind')!r}")
            if pos in seen:
                continue  # repeated inside this same batch
            seen.add(pos)
            if pos <= expected:
                # duplicate redelivery: store ignores identical content and
                # raises DuplicatePosError if the content differs
                self.store.append_log_entry(entry)
                continue
            if pos != expected + 1:
                return  # gap: leave cursor untouched, the loop re-polls
            self.store.append_log_entry(entry)
            expected = pos
        if expected != expected_after:
            self.store.advance_cursor(expected_after, expected)

    # -------------------------------------------------------------- scanners
    def _scan_loop(self, chunk_id: str) -> None:
        while not self._stop.is_set():
            chunk = self.store.get_chunk(chunk_id)
            if chunk is None or chunk["status"] == CH_RECONCILED:
                return
            try:
                if chunk["status"] == CH_NEW:
                    self.store.mark_scanning(chunk_id)
                    # durable hand-off point: the chunk is claimed, so a crash
                    # here must resume without restarting the source scan
                    self._crash_hook("after_scan_started")
                result = self._drive_scan(chunk_id)
                if result is None:
                    # parked against a gated test source: the phase advances
                    # only via the test control endpoint, so keep polling
                    if self._stop.wait(0.05):
                        return
                    continue
                if result["status"] != SCAN_DONE:
                    self._stop.wait(0.05)
                    continue
                self.store.save_scan_result(
                    chunk_id, result["L"], result["H"], result["rows"]
                )
                self._crash_hook("after_scan_received")
                self._reconcile(chunk_id)
                return
            except DuplicatePosError:
                self.store.set_failed()
                return
            except (RemoteError, http.client.HTTPException,
                    KeyError, ValueError, OSError):
                if self._stop.wait(0.2):
                    return
                continue

    def _drive_scan(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        """Two-phase scan; parked phases return None so the loop idles.

        A parked scan is the mock source's mechanism for delaying a response
        and interleaving writes/chunks deterministically.  Real sources would
        simply block inside the HTTP call, which the retry layer tolerates.
        """
        started = http_json(
            "POST", f"{self.source_url}/scan", {"chunk_id": chunk_id}
        )
        if started["status"] == SCAN_DONE:
            return started  # source already completed this chunk earlier
        current = started
        while current["status"] != SCAN_DONE:
            cont = http_json(
                "POST",
                f"{self.source_url}/scan/continue",
                {"chunk_id": chunk_id},
            )
            if cont["status"] == current["status"]:
                # parked: the test harness must advance the source phase
                return None
            current = cont
        return current

    def _reconcile(self, chunk_id: str) -> None:
        """Wait for the log through H, then materialize the chunk state at H."""
        from .protocol import dec_pos

        chunk = self.store.get_chunk(chunk_id)
        l = dec_pos(chunk["l"])
        h = dec_pos(chunk["h"])
        self._wait_for_cursor(h)
        sample = self.store.get_sample_rows(chunk_id)
        entries = self.store.entries_between(l, h)
        bounds = (chunk["lo"], chunk["hi"])
        merged = reconcile_chunk(sample, entries, range_bounds=bounds)
        self.store.replace_recon(chunk_id, merged)

    def _wait_for_cursor(self, target: int, timeout: Optional[float] = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._stop.is_set():
            meta = self.store.get_meta()
            if meta["failed"]:
                return False
            if meta["cursor"] >= target:
                return True
            self._wake.wait(0.1)
            self._wake.clear()
            if deadline is not None and time.monotonic() > deadline:
                return False
        return False

    # -------------------------------------------------------------- advance
    def advance(self, c: int) -> Dict[str, Any]:
        """Implementation of the advance/publish interface."""
        require_u64(c, "C")
        meta = self.store.get_meta()
        if meta["failed"]:
            return {"status": "failed", "reason": "loader is in failed state"}
        published = meta["published_c"]
        if published is not None and c < published:
            # a position below the already published one is rejected outright
            return {
                "status": "rejected",
                "reason": "C_below_published",
                "published_c": published,
            }
        if not self.store.all_reconciled():
            return {"status": "not_ready", "reason": "chunks_pending"}
        chunks = self.store.get_chunks()
        from .protocol import dec_pos

        max_h = max(dec_pos(ch["h"]) for ch in chunks)
        for ch in chunks:
            if dec_pos(ch["h"]) > c:
                return {"status": "not_ready", "reason": "C_below_chunk_H"}
        if meta["cursor"] < c:
            return {"status": "not_ready", "reason": "log_not_consumed_through_C"}
        if published == c:
            return {"status": "already_published", "C": c}
        # Build the source copy at C.  Each chunk's reconciled state is valid
        # at its own H.  For every write in (min L, C], apply it iff its key's
        # owning chunk has an H strictly below the write position: then the
        # write is not already reflected in that chunk's reconciled state.
        from .protocol import find_chunk

        ranges = meta["ranges"]
        h_by_chunk = {ch["chunk_id"]: dec_pos(ch["h"]) for ch in chunks}
        rows: Dict[str, Any] = {}
        for ch in chunks:
            for r in self.store.get_recon_rows(ch["chunk_id"]):
                rows[r["key"]] = r["value"]
        min_l = min(dec_pos(ch["l"]) for ch in chunks)
        for entry in self.store.entries_between(min_l, c):
            if entry["kind"] != "write":
                continue
            owner = str(find_chunk(entry["key"], ranges))
            if entry["pos"] <= h_by_chunk[owner]:
                continue  # already included in that chunk's reconciliation
            if entry["after"] is None:
                rows.pop(entry["key"], None)
            else:
                rows[entry["key"]] = entry["after"]["value"]
        ordered = [{"key": k, "value": rows[k]} for k in sorted(rows)]
        self._crash_hook("before_publish")
        self.store.publish(c, ordered, crash_hook=self._publish_crash_hook)
        self._crash_hook("after_publish_commit")
        return {"status": "published", "C": c}

    def _publish_crash_hook(self, point: str) -> None:
        self._crash_hook("in_publish_before_commit")

    def query(self) -> Dict[str, Any]:
        published_c, rows = self.store.get_replica()
        if published_c is None:
            return {"status": "not_ready"}
        return {"C": published_c, "rows": rows}

    def status(self) -> Dict[str, Any]:
        from .protocol import dec_pos

        meta = self.store.get_meta()
        return {
            "s": meta["s"],
            "cursor": meta["cursor"],
            "published_c": meta["published_c"],
            "failed": meta["failed"],
            "chunks": [
                {
                    "chunk_id": c["chunk_id"],
                    "status": c["status"],
                    "L": None if c["l"] is None else dec_pos(c["l"]),
                    "H": None if c["h"] is None else dec_pos(c["h"]),
                }
                for c in self.store.get_chunks()
            ],
        }
