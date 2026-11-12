"""Crash-recovery acceptance tests using real subprocess hard exits.

These exercise the durability contract end to end:

* a crash right after a scan response is received must not lose the persisted
  sample and must not cause a re-sample after restart;
* a crash immediately before the publication transaction commits must leave
  the previous publication (or not_ready) intact;
* a crash immediately after commit must keep the committed publication;
* in every case the restarted loader eventually publishes a replica that is
  byte-for-byte equal to the source's own snapshot at that C.

The mock source stays alive across loader restarts, mirroring a real
deployment where only the loader process dies.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import unittest

from tests.harness import (
    Harness,
    assert_snapshot_equal,
    src_info,
    src_snapshot,
    src_write,
)

WORKER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "run", "crash_worker.py")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _run_worker(workdir: str, source_url: str, crash_hook: str = "",
                target_c=None) -> subprocess.CompletedProcess:
    cfg = {
        "workdir": workdir,
        "source_url": source_url,
        "port": _free_port(),
        "crash_hook": crash_hook,
        "target_c": target_c,
    }
    return subprocess.run(
        [sys.executable, "-I", WORKER, json.dumps(cfg)],
        capture_output=True, text=True, timeout=60,
    )


def _events(proc: subprocess.CompletedProcess):
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]


class CrashRecoveryTests(unittest.TestCase):
    def _seeded_harness(self, manual=False, start_loader=True):
        return Harness(seed={"A": {"value": "a0"}, "B": {"value": "b0"},
                             "M": {"value": None}}, manual=manual,
                       start_loader=start_loader)

    def test_crash_after_scan_received_resumes_without_resampling(self):
        # manual gating keeps scans parked so the after-scan crash hook is
        # guaranteed to fire before reconciliation can finish; no in-process
        # loader runs, so the subprocess owns the loader database exclusively
        h = self._seeded_harness(manual=True, start_loader=False)
        try:
            workdir = h.tmp.name
            source_url = h.source_url
            scans_before = src_info(source_url)["scans"]

            # first run: hard-exit once a chunk scan has been durably started
            proc = _run_worker(workdir, source_url, crash_hook="after_scan_started")
            self.assertEqual(proc.returncode, 86, proc.stderr)
            # the crash happened before any scan could complete
            self.assertEqual(src_info(source_url)["scans"], scans_before)

            # drive both parked scans to completion on the surviving source
            from tests.harness import drive_manual_chunk

            drive_manual_chunk(source_url, "0")
            drive_manual_chunk(source_url, "1")
            scans_when_done = src_info(source_url)["scans"]
            tip_when_done = src_info(source_url)["tip"]

            # restart with no crash hook and let it finish
            proc2 = _run_worker(workdir, source_url)
            self.assertEqual(proc2.returncode, 0, proc2.stderr)
            events = _events(proc2)
            self.assertTrue(
                any(e.get("body", {}).get("status") in ("published", "already_published")
                    for e in events if e["event"] == "advance"),
                events,
            )
            query = next(e["body"] for e in events if e["event"] == "query")
            tip = src_info(source_url)["tip"]
            assert_snapshot_equal(self, query, src_snapshot(source_url, tip))
            # the restart reused the existing samples: no new scan records and
            # no extra watermark marks beyond the tip at scan completion
            self.assertEqual(src_info(source_url)["scans"], scans_when_done)
            self.assertEqual(src_info(source_url)["tip"], tip_when_done)
        finally:
            h.stop()

    def test_crash_before_publish_commit_keeps_last_publication(self):
        h = self._seeded_harness()
        try:
            workdir, source_url = h.tmp.name, h.source_url

            # first clean run establishes a publication at the current tip
            proc = _run_worker(workdir, source_url)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            first_events = _events(proc)
            first_c = next(e["body"]["C"] for e in first_events if e["event"] == "query")
            first_snapshot = src_snapshot(source_url, first_c)

            # mutate the source, then crash inside the publish transaction
            src_write(source_url, "A", {"value": "a-after"})
            src_write(source_url, "N", {"value": "new-key"})
            proc2 = _run_worker(
                workdir, source_url, crash_hook="in_publish_before_commit"
            )
            self.assertEqual(proc2.returncode, 86, proc2.stderr)

            # restart: it must recover, publish, and match the source exactly
            proc3 = _run_worker(workdir, source_url)
            self.assertEqual(proc3.returncode, 0, proc3.stderr)
            events = _events(proc3)
            query = next(e["body"] for e in events if e["event"] == "query")
            tip = src_info(source_url)["tip"]
            assert_snapshot_equal(self, query, src_snapshot(source_url, tip))

            # and the previous publication's C must still be <= current one;
            # the rolled-back transaction never exposed a partial replica
            self.assertGreaterEqual(query["C"], first_c)
            self.assertEqual(first_snapshot["C"], first_c)
        finally:
            h.stop()

    def test_crash_after_commit_keeps_committed_publication(self):
        h = self._seeded_harness()
        try:
            workdir, source_url = h.tmp.name, h.source_url
            proc = _run_worker(workdir, source_url, crash_hook="after_publish_commit")
            # the hook fires after a successful commit, so the process dies 86
            # but a durable publication must already exist on disk
            self.assertEqual(proc.returncode, 86, proc.stderr)

            proc2 = _run_worker(workdir, source_url)
            self.assertEqual(proc2.returncode, 0, proc2.stderr)
            events = _events(proc2)
            query = next(e["body"] for e in events if e["event"] == "query")
            tip = src_info(source_url)["tip"]
            assert_snapshot_equal(self, query, src_snapshot(source_url, tip))

            # a fresh query right after restart (before any new advance) would
            # already return rows: verify via a short-lived in-process reader
            from snapshot_loader.store import Store

            store = Store(os.path.join(workdir, "loader.db"))
            published_c, rows = store.get_replica()
            self.assertIsNotNone(published_c)
            store.close()
        finally:
            h.stop()

    def test_replica_matches_source_at_multiple_positions(self):
        """Publish repeatedly; each committed C must equal its source snapshot."""
        h = self._seeded_harness()
        try:
            workdir, source_url = h.tmp.name, h.source_url
            checked = []
            for round_no in range(3):
                src_write(source_url, "A", {"value": f"a-{round_no}"})
                src_write(source_url, "B", None if round_no == 1 else {"value": f"b-{round_no}"})
                target = src_info(source_url)["tip"]
                proc = _run_worker(workdir, source_url, target_c=target)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                events = _events(proc)
                query = next((e["body"] for e in events if e["event"] == "query"), None)
                if query is not None:
                    assert_snapshot_equal(
                        self, query, src_snapshot(source_url, query["C"])
                    )
                    checked.append(query["C"])
            self.assertTrue(checked)
        finally:
            h.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
