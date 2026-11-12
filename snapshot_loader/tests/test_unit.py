"""Unit tests for the pure protocol/store logic (no subprocesses, no network)."""

import os
import sqlite3
import tempfile
import unittest

from snapshot_loader.protocol import (
    UINT64_MAX,
    enc_pos,
    dec_pos,
    find_chunk,
    is_u64,
    key_in_range,
    validate_after,
)
from snapshot_loader.store import (
    DuplicatePosError,
    Store,
    apply_after,
    reconcile_chunk,
)


class U64Tests(unittest.TestCase):
    def test_accepts_plain_ints_in_range(self):
        self.assertTrue(is_u64(0))
        self.assertTrue(is_u64(UINT64_MAX))

    def test_rejects_bools_floats_negatives_and_overflows(self):
        for bad in (True, False, 1.0, -1, UINT64_MAX + 1, "3", None):
            self.assertFalse(is_u64(bad), bad)

    def test_position_encoding_is_lexicographically_ordered(self):
        values = [0, 1, 9, 10, 2 ** 63, UINT64_MAX]
        encoded = [enc_pos(v) for v in values]
        self.assertEqual(encoded, sorted(encoded))
        for v, e in zip(values, encoded):
            self.assertEqual(dec_pos(e), v)
            self.assertEqual(len(e), 20)


class RangeTests(unittest.TestCase):
    RANGES = [(None, "B"), ("B", None)]

    def test_half_open_membership(self):
        self.assertTrue(key_in_range("A", None, "B"))
        self.assertFalse(key_in_range("B", None, "B"))
        self.assertTrue(key_in_range("B", "B", None))
        self.assertTrue(key_in_range("ZZZ", "B", None))

    def test_find_chunk(self):
        self.assertEqual(find_chunk("A", self.RANGES), 0)
        self.assertEqual(find_chunk("B", self.RANGES), 1)
        self.assertEqual(find_chunk("ZZ", self.RANGES), 1)

    def test_after_validation(self):
        validate_after(None)
        validate_after({"value": "x"})
        validate_after({"value": None})
        for bad in ({"value": 1}, {}, {"value": "x", "extra": 1}, [], "x"):
            with self.assertRaises(Exception):
                validate_after(bad)


class ReconcileTests(unittest.TestCase):
    def test_write_then_delete_inside_interval_leaves_no_key(self):
        # Sample saw k present; (L, H] contains an insert of B then a delete.
        sample = [{"key": "B", "value": "sampled"}]
        entries = [
            {"pos": 5, "kind": "write", "key": "B", "after": {"value": "new"}},
            {"pos": 6, "kind": "mark"},
            {"pos": 7, "kind": "write", "key": "B", "after": None},
        ]
        self.assertEqual(reconcile_chunk(sample, entries), [])

    def test_untouched_keys_keep_sample_value(self):
        sample = [{"key": "A", "value": "a"}, {"key": "B", "value": "b"}]
        entries = [
            {"pos": 3, "kind": "write", "key": "B", "after": {"value": "b2"}},
        ]
        self.assertEqual(
            reconcile_chunk(sample, entries),
            [{"key": "A", "value": "a"}, {"key": "B", "value": "b2"}],
        )

    def test_last_write_wins_and_marks_are_noop(self):
        rows: dict = {}
        apply_after(rows, "k", {"value": "v1"})
        apply_after(rows, "k", {"value": "v2"})
        self.assertEqual(rows, {"k": "v2"})
        apply_after(rows, "k", None)
        self.assertEqual(rows, {})

    def test_real_null_value_is_distinct_from_deletion(self):
        sample = [{"key": "k", "value": None}]
        entries = [
            {"pos": 2, "kind": "write", "key": "k", "after": {"value": None}},
        ]
        self.assertEqual(reconcile_chunk(sample, entries), [{"key": "k", "value": None}])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "loader.db")
        self.store = Store(self.db)
        self.store.save_bootstrap(4, [(None, "B"), ("B", None)])

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _write(self, pos, kind="write", key="k", after=None):
        self.store.append_log_entry(
            {"pos": pos, "kind": kind, "key": key, "after": after}
            if kind == "write"
            else {"pos": pos, "kind": "mark"}
        )

    def test_duplicate_same_content_ignored_different_content_fails(self):
        self._write(1, after={"value": "a"})
        self._write(1, after={"value": "a"})  # identical redelivery: fine
        with self.assertRaises(DuplicatePosError):
            self._write(1, after={"value": "b"})  # same pos, new payload: hard fail

    def test_contiguous_cursor_and_interval_queries(self):
        # bootstrap s=4 means the cursor starts at 4 and the log tail is (4, ...]
        self.assertEqual(self.store.get_meta()["cursor"], 4)
        for pos in range(5, 10):
            self.store.append_log_entry({"pos": pos, "kind": "mark"})
        self.assertTrue(self.store.advance_cursor(4, 9))
        self.assertFalse(self.store.advance_cursor(4, 9))  # stale expected_old
        self.assertEqual(
            [e["pos"] for e in self.store.entries_between(6, 9)], [7, 8, 9]
        )

    def test_publish_is_atomic_and_keeps_last_publication_on_rollback(self):
        self.store.publish(3, [{"key": "k", "value": "v"}])
        c, rows = self.store.get_replica()
        self.assertEqual((c, rows), (3, [{"key": "k", "value": "v"}]))

        def boom(_point):
            raise RuntimeError("simulated crash before commit")

        with self.assertRaises(RuntimeError):
            self.store.publish(9, [{"key": "k", "value": "would-be-new"}], crash_hook=boom)
        # rolled back: previous publication still served
        c, rows = self.store.get_replica()
        self.assertEqual((c, rows), (3, [{"key": "k", "value": "v"}]))

    def test_persistence_survives_reopen(self):
        self.store.publish(2, [{"key": "k", "value": None}])
        self.store.close()
        reopened = Store(self.db)
        c, rows = reopened.get_replica()
        self.assertEqual(c, 2)
        self.assertEqual(rows, [{"key": "k", "value": None}])
        reopened.close()

    def test_sqlite_text_collation_matches_ascii_ordering(self):
        conn = sqlite3.connect(":memory:")
        keys = ["A", "B", "BA", "BB", "C", "a"]
        conn.execute("CREATE TABLE t(k TEXT)")
        conn.executemany("INSERT INTO t VALUES(?)", [(k,) for k in keys])
        db_order = [r[0] for r in conn.execute("SELECT k FROM t ORDER BY k")]
        self.assertEqual(db_order, sorted(keys))


if __name__ == "__main__":
    unittest.main(verbosity=2)
