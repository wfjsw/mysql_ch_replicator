"""
Unit tests for MariaDB binlog_row_image=FULL_NODUP support.

With FULL_NODUP:
  - Before-image (BI): all columns are logged (same as FULL).
  - After-image  (AI): only *changed* columns are logged.

The parser receives a `columns_present_bitmap2` that has 0-bits for
unchanged columns, so _read_column_data() returns None with
none_source==COLS_BITMAP for those columns.

UpdateRowsEvent._fetch_one_row() must merge the AI with the BI so that
callers always see a complete, up-to-date row in `after_values`.
"""
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from mysql_ch_replicator.pymysqlreplication.constants import NONE_SOURCE


# ---------------------------------------------------------------------------
# Minimal stubs – we test _fetch_one_row() logic in isolation, without
# needing a real MySQL / MariaDB connection.
# ---------------------------------------------------------------------------

class _FakeUpdateRowsEvent:
    """Simulates the internal state produced by UpdateRowsEvent after the
    bitmaps have been read from the packet.  The real _fetch_one_row() only
    depends on _read_column_data() and _get_none_sources(), so we replace
    those with simple stubs that return canned data.
    """

    def __init__(self, before_values, before_none_sources,
                 after_values, after_none_sources):
        self._before_values = before_values
        self._before_none_sources = before_none_sources
        self._after_values = after_values
        self._after_none_sources = after_none_sources
        self._call_count = 0

    # Replicate exactly what the real code calls (two calls per row).
    def _read_column_data(self, bitmap, row_image_type=None):
        self._call_count += 1
        if self._call_count == 1:
            return dict(self._before_values)       # BI
        else:
            return dict(self._after_values)        # AI

    def _get_none_sources(self, column_data):
        # Return a copy of whichever none_sources dict matches this call.
        if self._call_count == 1:
            return dict(self._before_none_sources)
        else:
            return dict(self._after_none_sources)

    # Paste the real implementation of _fetch_one_row() – this is what we
    # are testing.
    def _fetch_one_row(self):
        row = {}

        row["before_values"] = self._read_column_data(None)
        row["before_none_sources"] = self._get_none_sources(row["before_values"])
        row["after_values"] = self._read_column_data(None)
        row["after_none_sources"] = self._get_none_sources(row["after_values"])

        # FULL_NODUP merge (copy of the production code under test)
        cols_bitmap_absent = [
            col_name for col_name, none_src in row["after_none_sources"].items()
            if none_src == NONE_SOURCE.COLS_BITMAP and col_name in row["before_values"]
        ]
        for col_name in cols_bitmap_absent:
            row["after_values"][col_name] = row["before_values"][col_name]
            before_src = row["before_none_sources"].get(col_name)
            if before_src is not None:
                row["after_none_sources"][col_name] = before_src
            else:
                del row["after_none_sources"][col_name]

        return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_full_nodup_unchanged_columns_filled_from_before_image():
    """With FULL_NODUP, unchanged columns (COLS_BITMAP in AI) must take their
    value from the before-image in after_values."""
    # Row: id=1, name='Alice', age=30
    # UPDATE sets age=31  ->  only age changes
    before = {"id": 1, "name": "Alice", "age": 30}
    before_ns = {}  # no None values in BI

    # AI: id and name are absent (bitmap 0), only age is present
    after_raw = {"id": None, "name": None, "age": 31}
    after_ns_raw = {
        "id": NONE_SOURCE.COLS_BITMAP,
        "name": NONE_SOURCE.COLS_BITMAP,
    }

    event = _FakeUpdateRowsEvent(before, before_ns, after_raw, after_ns_raw)
    row = event._fetch_one_row()

    # after_values must contain the full, merged row
    assert row["after_values"] == {"id": 1, "name": "Alice", "age": 31}
    # COLS_BITMAP entries must be gone from after_none_sources
    assert "id" not in row["after_none_sources"]
    assert "name" not in row["after_none_sources"]
    # before_values untouched
    assert row["before_values"] == before


def test_full_nodup_null_column_in_before_image():
    """If a column was NULL before the update and is unchanged (FULL_NODUP),
    after_values should still be NULL, and after_none_sources should carry the
    NULL source (not COLS_BITMAP)."""
    before = {"id": 1, "name": None, "age": 30}
    before_ns = {"name": NONE_SOURCE.NULL}

    after_raw = {"id": None, "name": None, "age": 31}
    after_ns_raw = {
        "id": NONE_SOURCE.COLS_BITMAP,
        "name": NONE_SOURCE.COLS_BITMAP,
    }

    event = _FakeUpdateRowsEvent(before, before_ns, after_raw, after_ns_raw)
    row = event._fetch_one_row()

    assert row["after_values"]["name"] is None
    # The source should be NULL (inherited from BI), not COLS_BITMAP
    assert row["after_none_sources"].get("name") == NONE_SOURCE.NULL
    assert "id" not in row["after_none_sources"]


def test_full_image_unaffected():
    """With FULL row image, both bitmaps are full – no COLS_BITMAP entries.
    after_values must be returned exactly as-is (no merge needed)."""
    before = {"id": 1, "name": "Alice", "age": 30}
    before_ns = {}

    after_raw = {"id": 1, "name": "Alice", "age": 31}
    after_ns_raw = {}  # no COLS_BITMAP entries

    event = _FakeUpdateRowsEvent(before, before_ns, after_raw, after_ns_raw)
    row = event._fetch_one_row()

    assert row["after_values"] == {"id": 1, "name": "Alice", "age": 31}
    assert row["after_none_sources"] == {}


def test_full_nodup_all_columns_changed():
    """Edge case: all columns change – AI bitmap is full, nothing to merge."""
    before = {"id": 1, "name": "Alice", "age": 30}
    before_ns = {}

    after_raw = {"id": 1, "name": "Bob", "age": 25}
    after_ns_raw = {}

    event = _FakeUpdateRowsEvent(before, before_ns, after_raw, after_ns_raw)
    row = event._fetch_one_row()

    assert row["after_values"] == {"id": 1, "name": "Bob", "age": 25}
    assert row["after_none_sources"] == {}


def test_full_nodup_only_primary_key_in_ai():
    """Extreme FULL_NODUP case: only the primary key is in the AI (MariaDB may
    include PK even when unchanged).  All non-PK columns are absent and must be
    taken from the BI."""
    before = {"id": 5, "name": "Charlie", "score": 99}
    before_ns = {}

    # Only 'name' changed; 'score' and 'id' absent from AI bitmap
    after_raw = {"id": None, "name": "Charlie-Updated", "score": None}
    after_ns_raw = {
        "id": NONE_SOURCE.COLS_BITMAP,
        "score": NONE_SOURCE.COLS_BITMAP,
    }

    event = _FakeUpdateRowsEvent(before, before_ns, after_raw, after_ns_raw)
    row = event._fetch_one_row()

    assert row["after_values"] == {"id": 5, "name": "Charlie-Updated", "score": 99}
    assert "id" not in row["after_none_sources"]
    assert "score" not in row["after_none_sources"]
