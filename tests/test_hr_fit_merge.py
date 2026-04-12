"""Tests for Apple HR + FIT merge engine."""

from pathlib import Path
import sys
import struct
from unittest.mock import MagicMock, patch

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import hr_fit_merge


def test_merge_without_overwrite_preserves_existing_hr():
    fit_records = [
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 140},
        {"timestamp": "2026-04-11T07:01:06Z", "hr": None},
    ]
    apple = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}, {"timestamp": "2026-04-11T07:01:06Z", "hr": 151}]
    merged, report = hr_fit_merge.merge_fit_with_hr(fit_records, apple, hr_fit_merge.MergeOptions(overwrite_existing_hr=False))

    assert merged[0]["hr"] == 140
    assert merged[1]["hr"] == 151
    assert report["hr_points_written"] == 1


def test_merge_with_overwrite_replaces_existing_hr():
    fit_records = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 140}]
    apple = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}]
    merged, report = hr_fit_merge.merge_fit_with_hr(fit_records, apple, hr_fit_merge.MergeOptions(overwrite_existing_hr=True, tolerance_seconds=2))

    assert merged[0]["hr"] == 150
    assert report["hr_points_written"] == 1


def test_parse_fit_records_for_merge_uses_utc_and_hr_optional():
    from datetime import datetime, timezone

    class F:
        def __init__(self, name, value):
            self.name = name
            self.value = value

    fake_msg = [[F("timestamp", datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)), F("heart_rate", None)]]

    class _FakeFit:
        def parse(self):
            return None

        def get_messages(self, name):
            assert name == "record"
            return fake_msg

    with patch("hr_fit_merge.FitFile", return_value=_FakeFit()):
        out = hr_fit_merge.parse_fit_records_for_merge(b"abc")

    assert out["summary"]["sample_count"] == 1
    assert out["summary"]["has_existing_hr"] is False


def test_rewrite_fit_hr_values_patches_binary_record_hr():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")

    # FIT definition message for global message 20 (record) with fields: timestamp(uint32), heart_rate(uint8)
    definition = bytes([
        0x40,  # normal header, definition, local msg 0
        0x00,  # reserved
        0x00,  # little endian
        0x14, 0x00,  # global msg number 20
        0x02,  # 2 fields
        0xFD, 0x04, 0x86,  # field 253 timestamp uint32
        0x03, 0x01, 0x02,  # field 3 heart_rate uint8
    ])
    data_msg = bytes([0x00]) + struct.pack("<I", ts) + bytes([100])
    data = definition + data_msg

    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    crc = struct.pack("<H", hr_fit_merge._fit_crc(header + data))
    fit_bytes = header + data + crc

    merged_records = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 1
    # final HR byte in the only data message should now be 150
    assert out_bytes[-3] == 150


def test_compressed_record_uses_timestamp_from_non_record_message():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")

    # Local 0: global 18 (session-like), includes timestamp only
    def_session = bytes([
        0x40, 0x00, 0x00, 0x12, 0x00, 0x01,
        0xFD, 0x04, 0x86,
    ])
    msg_session = bytes([0x00]) + struct.pack("<I", ts)

    # Local 1: global 20 (record), includes heart_rate only; timestamp provided via compressed header
    def_record = bytes([
        0x41, 0x00, 0x00, 0x14, 0x00, 0x01,
        0x03, 0x01, 0x02,
    ])
    # compressed header for local=1,time offset=1
    msg_record_compressed = bytes([0xA1, 100])

    data = def_session + msg_session + def_record + msg_record_compressed
    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    crc = struct.pack("<H", hr_fit_merge._fit_crc(header + data))
    fit_bytes = header + data + crc

    compressed_ts = (ts & ~0x1F) + 1
    if compressed_ts < ts:
        compressed_ts += 0x20
    compressed_iso = (
        hr_fit_merge.FIT_EPOCH + __import__("datetime").timedelta(seconds=compressed_ts)
    ).isoformat().replace("+00:00", "Z")
    merged_records = [{"timestamp": compressed_iso, "hr": 151}]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 1
    assert out_bytes[-3] == 151
