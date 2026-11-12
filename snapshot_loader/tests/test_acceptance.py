"""End-to-end acceptance tests (in-process source + loader over real HTTP)."""

from __future__ import annotations

import random
import threading
import unittest

from snapshot_loader.store import CH_RECONCILED, DuplicatePosError

from tests.harness import (
    Harness,
    assert_snapshot_equal,
    drive_manual_chunk,
    loader_advance,
    loader_query,
    scan_continue,
    scan_control,
    scan_start,
    src_info,
    src_mark,
    src_snapshot,
    src_write,
    advance_until,
    wait_all_reconciled,
    wait_chunk_status,
    wait_cursor,
)


class BasicFlowTests(unittest.TestCase):
    def test_full_publish_matches_source_snapshot_and_query_is_ordered(self):
        h = Harness(seed={"A": {"value": "a0"}, "B": {"value": None},
                          "C": {"value": "c0"}})
        try:
            wait_all_reconciled(h.loader_url)
            # scan marks also occupy pos: refresh the tip after scans finish
            tip = src_info(h.source_url)["tip"]
            wait_cursor(h.loader_url, tip)
            body = advance_until(h.loader_url, tip)
            status, q = loader_query(h.loader_url)
            self.assertEqual(status, 200)
            assert_snapshot_equal(self, q, src_snapshot(h.source_url, tip))
            # real null value survives as value=None and key remains present
            keys = {r["key"]: r["value"] for r in q["rows"]}
            self.assertIn("B", keys)
            self.assertIsNone(keys["B"])
            # rows are in ASCII key order
            self.assertEqual([r["key"] for r in q["rows"]],
                             sorted(r["key"] for r in q["rows"]))
        finally:
            h.stop()

    def test_query_not_ready_before_first_publication(self):
        h = Harness(seed={"A": {"value": "a0"}})
        try:
            status, body = loader_query(h.loader_url)
            self.assertEqual(status, 409)
            self.assertEqual(body, {"status": "not_ready"})
        finally:
            h.stop()


class WatermarkReconciliationTests(unittest.TestCase):
    def test_write_after_L_then_sample_then_delete_before_H_leaves_no_key(self):
        """Headline acceptance scenario from the specification."""
        h = Harness(seed={"A": {"value": "a0"}, "C": {"value": "c0"}}, manual=True)
        try:
            s, l = h.source_url, h.loader_url
            started = scan_start(s, "1")  # chunk 1 owns [B, infinity)
            low = started["L"]

            p_write = src_write(s, "B", {"value": "v1"})   # write k=B after L
            self.assertGreater(p_write, low)
            sampled = scan_control(s, "1", "sampled")
            self.assertEqual(sampled["status"], "sampled")
            p_del = src_write(s, "B", None)                # delete k before H
            done = scan_control(s, "1", "done")
            high = done["H"]
            self.assertLess(low, p_write)
            self.assertLess(p_write, p_del)
            self.assertLess(p_del, high)

            # the single sample really did observe B
            self.assertIn("B", [r["key"] for r in done["rows"]])
            sample_b = next(r for r in done["rows"] if r["key"] == "B")
            self.assertEqual(sample_b["value"], "v1")

            wait_chunk_status(l, "1", CH_RECONCILED)
            recon = h.store.get_recon_rows("1")
            self.assertNotIn("B", [r["key"] for r in recon],
                             "deleted key must not survive reconciliation")

            # finish chunk 0 and publish: whole replica must match the source
            drive_manual_chunk(s, "0")
            wait_chunk_status(l, "0", CH_RECONCILED)
            tip = src_info(s)["tip"]
            wait_cursor(l, tip)
            status, body = loader_advance(l, tip)
            self.assertEqual(body["status"], "published")
            _, q = loader_query(l)
            assert_snapshot_equal(self, q, src_snapshot(s, tip))
        finally:
            h.stop()

    def test_stale_sample_is_overwritten_by_write_before_H(self):
        h = Harness(seed={"B": {"value": "b0"}}, manual=True)
        try:
            s, l = h.source_url, h.loader_url
            scan_start(s, "1")
            scan_control(s, "1", "sampled")  # sample sees the old b0
            src_write(s, "B", {"value": "b1"})  # overwrite before H
            done = scan_control(s, "1", "done")
            self.assertEqual(
                [r for r in done["rows"] if r["key"] == "B"],
                [{"key": "B", "value": "b0"}],
            )
            wait_chunk_status(l, "1", CH_RECONCILED)
            recon = {r["key"]: r["value"] for r in h.store.get_recon_rows("1")}
            self.assertEqual(recon.get("B"), "b1")  # log repair wins
        finally:
            h.stop()

    def test_two_chunks_interleaved_with_post_H_writes_and_deletion(self):
        h = Harness(seed={"A": {"value": "a0"}, "B": {"value": "b0"}}, manual=True)
        try:
            s, l = h.source_url, h.loader_url
            l0 = scan_start(s, "0")["L"]
            l1 = scan_start(s, "1")["L"]  # marks interleave across chunks
            src_write(s, "A", {"value": "a1"})
            scan_control(s, "0", "sampled")          # chunk 0 sample sees a1
            src_write(s, "B", {"value": "b1"})
            scan_control(s, "1", "sampled")          # chunk 1 sample sees b1
            src_write(s, "A", {"value": "a2"})       # after chunk0 sample
            done1 = scan_control(s, "1", "done")     # H1 first
            h1 = done1["H"]
            p_del = src_write(s, "B", None)          # strictly after H1
            done0 = scan_control(s, "0", "done")     # H0 later
            h0 = done0["H"]
            self.assertLess(l0, h0)
            self.assertLess(l1, h1)
            self.assertGreater(p_del, h1)

            wait_chunk_status(l, "0", CH_RECONCILED)
            wait_chunk_status(l, "1", CH_RECONCILED)
            tip = src_info(s)["tip"]
            wait_cursor(l, tip)
            status, body = loader_advance(l, tip)
            self.assertEqual(body["status"], "published")
            _, q = loader_query(l)
            expected = src_snapshot(s, tip)
            assert_snapshot_equal(self, q, expected)
            # explicit expectations: A=a2, B deleted after H1 but replayed
            keys = {r["key"]: r["value"] for r in q["rows"]}
            self.assertEqual(keys, {"A": "a2"})
        finally:
            h.stop()


class ScanRetryTests(unittest.TestCase):
    def test_repeated_calls_return_immutable_sample(self):
        h = Harness(seed={"A": {"value": "a0"}, "B": {"value": "b0"}}, manual=True)
        try:
            s = h.source_url
            first = scan_start(s, "0")
            again = scan_start(s, "0")
            self.assertEqual(first, again)  # same L, no duplicate mark
            scan_control(s, "0", "sampled")
            parked = scan_continue(s, "0")
            self.assertEqual(parked["status"], "sampled")  # manual gate holds
            done = scan_control(s, "0", "done")
            retry = scan_continue(s, "0")
            restart = scan_start(s, "0")
            self.assertEqual(retry, done)
            self.assertEqual(restart, done)  # one sample, fixed forever
        finally:
            h.stop()

    def test_restart_resumes_without_resampling_and_publishes(self):
        h = Harness(seed={"A": {"value": "a0"}, "B": {"value": "b0"}}, manual=True)
        try:
            s, l = h.source_url, h.loader_url
            # complete chunk 0, park chunk 1 after L only
            done0 = drive_manual_chunk(s, "0")
            scan_start(s, "1")
            wait_chunk_status(l, "0", CH_RECONCILED)
            before = src_info(s)

            # tear the whole engine down and reopen the same databases; the
            # old daemon threads are stopped so they cannot race the new ones
            h.loader_server.engine.stop()
            h.loader_server.shutdown()
            h.loader_server.server_close()
            h.store.close()
            from tests.harness import build_loader_server

            h.loader_server = build_loader_server(
                h.loader_db, s, "127.0.0.1", 0
            )
            t = threading.Thread(
                target=h.loader_server.serve_forever,
                kwargs={"poll_interval": 0.05}, daemon=True,
            )
            t.start()
            h._servers.append(h.loader_server)
            h.loader_url = (
                f"http://127.0.0.1:{h.loader_server.server_address[1]}"
            )
            h.store = h.loader_server.engine.store

            # source scan records must be untouched by the restart
            self.assertEqual(src_info(s)["scans"], before["scans"])
            drive_manual_chunk(s, "1")
            wait_all_reconciled(h.loader_url)
            tip = src_info(s)["tip"]  # refresh after chunk 1's scan marks
            body = advance_until(h.loader_url, tip)
            self.assertEqual(body["status"], "published")
            _, q = loader_query(h.loader_url)
            assert_snapshot_equal(self, q, src_snapshot(s, tip))
        finally:
            h.stop()


class AdvanceContractTests(unittest.TestCase):
    def test_reject_below_published_and_idempotent_at_published(self):
        h = Harness(seed={"A": {"value": "a0"}})
        try:
            l = h.loader_url
            wait_all_reconciled(l)
            tip = src_info(h.source_url)["tip"]  # includes the scan mark positions
            wait_cursor(l, tip)
            advance_until(l, tip)
            status, body = loader_advance(l, tip - 1)
            self.assertEqual(status, 409)
            self.assertEqual(body["status"], "rejected")
            status, body = loader_advance(l, tip)
            self.assertEqual(body["status"], "already_published")
        finally:
            h.stop()

    def test_not_ready_when_chunk_pending_or_log_behind(self):
        h = Harness(seed={"A": {"value": "a0"}}, manual=True)
        try:
            l, s = h.loader_url, h.source_url
            tip = src_info(s)["tip"]
            status, body = loader_advance(l, tip)
            self.assertEqual(body["status"], "not_ready")
            self.assertEqual(body["reason"], "chunks_pending")

            drive_manual_chunk(s, "0")
            drive_manual_chunk(s, "1")
            wait_chunk_status(l, "0", CH_RECONCILED)
            wait_chunk_status(l, "1", CH_RECONCILED)
            current_tip = src_info(s)["tip"]
            wait_cursor(l, current_tip)
            # ask beyond the consumed log tip to exercise the log-behind branch
            future = current_tip + 50
            status, body = loader_advance(l, future)
            self.assertEqual(body["status"], "not_ready")
            self.assertEqual(body["reason"], "log_not_consumed_through_C")
        finally:
            h.stop()

    def test_c_below_chunk_H_rejected_as_not_ready(self):
        h = Harness(seed={"A": {"value": "a0"}}, manual=True)
        try:
            l, s = h.loader_url, h.source_url
            done0 = drive_manual_chunk(s, "0")
            done1 = drive_manual_chunk(s, "1")
            max_h = max(done0["H"], done1["H"])
            wait_chunk_status(l, "0", CH_RECONCILED)
            wait_chunk_status(l, "1", CH_RECONCILED)
            wait_cursor(l, max_h)
            status, body = loader_advance(l, max_h - 1)
            self.assertEqual(body["status"], "not_ready")
            self.assertEqual(body["reason"], "C_below_chunk_H")
        finally:
            h.stop()


class LogStreamTests(unittest.TestCase):
    def test_gap_makes_consumer_wait_then_contiguous_resumption(self):
        # no background threads: deterministic cursor manipulation
        h2 = Harness(seed={"A": {"value": "a0"}}, auto_scan=False)
        try:
            engine = h2.loader_server.engine
            store = h2.store
            # the cursor starts at the bootstrap s: the log begins after s
            cursor = store.get_meta()["cursor"]
            self.assertEqual(cursor, store.get_meta()["s"])
            missing = cursor + 1
            ahead = cursor + 2
            ahead_entry = {"pos": ahead, "kind": "mark"}
            # the future pos arrives while `missing` is absent: cursor stays
            engine._consume_entries([dict(ahead_entry)], expected_after=cursor)
            self.assertEqual(store.get_meta()["cursor"], cursor)
            # identical redelivery of the buffered future entry is a no-op
            store.append_log_entry(dict(ahead_entry))
            # the gap fills; the buffered duplicate is ignored, cursor jumps
            engine._consume_entries(
                [
                    {"pos": missing, "kind": "mark"},
                    dict(ahead_entry),
                ],
                expected_after=cursor,
            )
            self.assertEqual(store.get_meta()["cursor"], ahead)
        finally:
            h2.stop()

    def test_duplicate_pos_with_different_content_fails_and_keeps_last_publish(self):
        h = Harness(seed={"A": {"value": "a0"}})
        try:
            l, s = h.loader_url, h.source_url
            wait_all_reconciled(l)
            tip = src_info(s)["tip"]
            wait_cursor(l, tip)
            advance_until(l, tip)
            engine = h.loader_server.engine
            stored = h.store.entries_between(0, tip)[0]
            corrupted = dict(stored)
            if corrupted["kind"] == "write":
                corrupted["after"] = None  # same pos, different payload
            else:
                corrupted["kind"] = "write"
                corrupted["key"] = "Z"
                corrupted["after"] = {"value": "z"}
            with self.assertRaises(DuplicatePosError):
                h.store.append_log_entry(corrupted)
            h.store.set_failed()  # what the log loop does on that error
            status, body = loader_advance(l, tip + 1)
            self.assertEqual(status, 500)
            self.assertEqual(body["status"], "failed")
            # the last successful publication remains queryable
            _, q = loader_query(l)
            self.assertEqual(q["C"], tip)
            assert_snapshot_equal(self, q, src_snapshot(s, tip))
        finally:
            h.stop()


class ConcurrentFuzzTest(unittest.TestCase):
    def test_concurrent_writers_autonomous_scanning_matches_every_source_snapshot(self):
        h = Harness(seed={"A": {"value": "init"}, "B": {"value": "init"},
                          "M": {"value": None}})
        try:
            s, l = h.source_url, h.loader_url
            rng = random.Random(20261112)
            keys = ["A", "AA", "B", "BA", "C", "a"]  # straddle the B boundary
            stop = threading.Event()

            def writer():
                while not stop.is_set():
                    key = rng.choice(keys)
                    roll = rng.random()
                    if roll < 0.25:
                        src_write(s, key, None)                    # deletion
                    elif roll < 0.4:
                        src_write(s, key, {"value": None})         # real null
                    else:
                        src_write(s, key, {"value": str(rng.randint(0, 9999))})
                    if rng.random() < 0.2:
                        src_mark(s)

            threads = [threading.Thread(target=writer, daemon=True) for _ in range(3)]
            for t in threads:
                t.start()
            # let the initial chunk scans finish while writes keep flowing
            wait_all_reconciled(l)

            # publish at several points while writes continue; always pick a
            # fresh tip so C is never below any chunk's H
            published = []
            for _ in range(4):
                target = src_info(s)["tip"]
                body = advance_until(l, target)
                self.assertIn(body["status"], ("published", "already_published"))
                published.append(body["C"])
                # verify the just-published replica equals the source at that C
                _, q = loader_query(l)
                assert_snapshot_equal(self, q, src_snapshot(s, body["C"]))
            stop.set()
            for t in threads:
                t.join(5)
            final_tip = src_info(s)["tip"]
            body = advance_until(l, final_tip)
            self.assertIn(body["status"], ("published", "already_published"))
            _, q = loader_query(l)
            assert_snapshot_equal(self, q, src_snapshot(s, final_tip))
            # every earlier publication matched the source at its own C
            self.assertEqual([src_snapshot(s, c)["C"] for c in published], published)
        finally:
            h.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
