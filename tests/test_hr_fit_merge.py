"""Tests for Apple HR + FIT merge engine."""

from pathlib import Path
import sys
import struct
from unittest.mock import MagicMock, patch
import pytest

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
        {"timestamp": "2026-04-11T07:01:07Z", "hr": None},
    ]
    apple = [
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 150},
        {"timestamp": "2026-04-11T07:01:07Z", "hr": 154},
    ]
    merged, report = hr_fit_merge.merge_fit_with_hr(
        fit_records,
        apple,
        hr_fit_merge.MergeOptions(overwrite_existing_hr=False),
    )

    assert merged[0]["hr"] == 140
    assert merged[1]["hr"] == 152
    assert merged[2]["hr"] == 154
    assert report["hr_points_written"] == 2


def test_parse_apple_hr_payload_details_supports_data_workouts_structure():
    payload = b"""{
      "data": {
        "selectedWorkoutId": "wk2",
        "workouts": [
          {
            "id": "wk1",
            "start": "2026-04-11 05:00:00 +0000",
            "end": "2026-04-11 06:00:00 +0000",
            "heartRateData": [{"date":"2026-04-11 09:00:00 +0200","Avg":111,"units":"bpm"}]
          },
          {
            "id": "wk2",
            "start": "2026-04-11 07:00:00 +0000",
            "end": "2026-04-11 08:00:00 +0000",
            "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]
          }
        ]
      }
    }"""
    parsed = hr_fit_merge.parse_apple_hr_payload_details(
        payload,
        source_type="json",
        fit_start_time="2026-04-11T07:00:00Z",
        fit_end_time="2026-04-11T08:00:00Z",
    )
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["workouts_found"] == 2
    assert parsed["debug"]["selected_workout_has_heart_rate_data"] is True
    assert parsed["debug"]["extracted_hr_points"] == 1


def test_parse_apple_hr_payload_details_csv_includes_debug_envelope():
    payload = b"timestamp,hr\n2026-04-11T07:01:06Z,126\n"
    parsed = hr_fit_merge.parse_apple_hr_payload_details(payload, source_type="csv")
    assert parsed["source_type"] == "csv"
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["parser_mode"] == "csv"
    assert parsed["debug"]["extracted_hr_points"] == 1


def test_parse_apple_hr_payload_details_csv_tracks_rejections():
    payload = b"timestamp,hr\n2026-04-11T07:01:06Z,126\nbad-date,130\n2026-04-11T07:01:07Z,bad\n"
    parsed = hr_fit_merge.parse_apple_hr_payload_details(payload, source_type="csv")
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["raw_heart_rate_entries_found"] == 3
    assert parsed["debug"]["parsed_heart_rate_entries_count"] == 1
    assert parsed["debug"]["rejected_entries_count"] == 2
    assert parsed["debug"]["rejection_reasons"]["Invalid timestamp format: bad-date"] == 1
    assert parsed["debug"]["rejection_reasons"]["Invalid HR value: bad"] == 1


def test_parse_apple_hr_payload_details_json_with_bom_and_alias_fields():
    payload = (
        b"\xef\xbb\xbf"
        b'{"samples":[{"startDate":"2026-04-11T07:01:06Z","value":126},{"time":"2026-04-11T07:01:07Z","bpm":127}]}'
    )
    parsed = hr_fit_merge.parse_apple_hr_payload_details(payload, source_type="json")
    assert parsed["samples"] == [
        {"timestamp": "2026-04-11T07:01:06Z", "hr": 126},
        {"timestamp": "2026-04-11T07:01:07Z", "hr": 127},
    ]
    assert parsed["debug"]["raw_heart_rate_entries_found"] == 2


def test_merge_with_overwrite_replaces_existing_hr():
    fit_records = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 140}]
    apple = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}]
    merged, report = hr_fit_merge.merge_fit_with_hr(
        fit_records,
        apple,
        hr_fit_merge.MergeOptions(overwrite_existing_hr=True),
    )

    assert merged[0]["hr"] == 150
    assert report["hr_points_written"] == 1


def test_interpolate_hr_no_extrapolation_and_no_zero_fill():
    out = hr_fit_merge.interpolate_hr(
        [
            {"timestamp": "2026-04-11T07:01:05Z", "hr": 150},
            {"timestamp": "2026-04-11T07:01:07Z", "hr": 154},
        ],
        [
            "2026-04-11T07:01:04Z",
            "2026-04-11T07:01:05Z",
            "2026-04-11T07:01:06Z",
            "2026-04-11T07:01:07Z",
            "2026-04-11T07:01:08Z",
        ],
    )
    assert out == [None, 150, 152, 154, None]
    assert 0 not in [v for v in out if v is not None]


def test_render_merged_output_json_serializes_payload():
    content = hr_fit_merge.render_merged_output_json(
        "ride.fit",
        [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}],
        {"hr_points_written": 1},
    )
    assert b'"source_fit": "ride.fit"' in content
    assert b'"hr_points_written": 1' in content


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
    crc = struct.pack("<H", hr_fit_merge._fit_crc(data))
    fit_bytes = header + data + crc

    merged_records = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 1
    # final HR byte in the only data message should now be 150
    assert out_bytes[-3] == 150


def test_rewrite_fit_hr_values_handles_developer_field_sizes():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")

    # FIT definition with developer-data bit set and one 2-byte developer field
    definition = bytes([
        0x60,  # normal header, definition + developer-data, local msg 0
        0x00,  # reserved
        0x00,  # little endian
        0x14, 0x00,  # global msg number 20
        0x02,  # 2 standard fields
        0xFD, 0x04, 0x86,  # field 253 timestamp uint32
        0x03, 0x01, 0x02,  # field 3 heart_rate uint8
        0x01,  # 1 developer field definition follows
        0x00, 0x02, 0x00,  # dev field num, size=2, dev data index
    ])
    data_msg = bytes([0x00]) + struct.pack("<I", ts) + bytes([100]) + bytes([0xAA, 0xBB])
    data = definition + data_msg

    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    crc = struct.pack("<H", hr_fit_merge._fit_crc(data))
    fit_bytes = header + data + crc

    merged_records = [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 1
    assert out_bytes[-5] == 150


def test_rewrite_fit_hr_values_uses_per_record_hr_for_duplicate_timestamps():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")

    definition = bytes([
        0x40,  # normal header, definition, local msg 0
        0x00,  # reserved
        0x00,  # little endian
        0x14, 0x00,  # global msg number 20
        0x02,  # 2 fields
        0xFD, 0x04, 0x86,  # field 253 timestamp uint32
        0x03, 0x01, 0x02,  # field 3 heart_rate uint8
    ])
    msg_1 = bytes([0x00]) + struct.pack("<I", ts) + bytes([100])
    msg_2 = bytes([0x00]) + struct.pack("<I", ts) + bytes([101])
    data = definition + msg_1 + msg_2

    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    crc = struct.pack("<H", hr_fit_merge._fit_crc(data))
    fit_bytes = header + data + crc

    merged_records = [
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 111},
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 112},
    ]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 2
    payload = out_bytes[12:-2]
    assert payload[len(definition) + 1 + 4] == 111
    assert payload[len(definition) + (1 + 4 + 1) + 1 + 4] == 112


def test_rewrite_fit_hr_values_skips_non_writable_record_in_fifo_order():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")

    # local msg 0: record with timestamp only (no HR field, not writable)
    definition_no_hr = bytes([
        0x40,  # definition, local msg 0
        0x00,
        0x00,
        0x14, 0x00,
        0x01,
        0xFD, 0x04, 0x86,
    ])
    # local msg 1: record with timestamp + HR field (writable)
    definition_with_hr = bytes([
        0x41,  # definition, local msg 1
        0x00,
        0x00,
        0x14, 0x00,
        0x02,
        0xFD, 0x04, 0x86,
        0x03, 0x01, 0x02,
    ])
    # first record (no HR byte), second record (has HR byte)
    msg_no_hr = bytes([0x00]) + struct.pack("<I", ts)
    msg_with_hr = bytes([0x01]) + struct.pack("<I", ts) + bytes([100])
    data = definition_no_hr + definition_with_hr + msg_no_hr + msg_with_hr

    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    crc = struct.pack("<H", hr_fit_merge._fit_crc(data))
    fit_bytes = header + data + crc

    merged_records = [
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 111},  # maps to non-writable record
        {"timestamp": "2026-04-11T07:01:05Z", "hr": 112},  # should patch writable record
    ]
    out_bytes, patched = hr_fit_merge.rewrite_fit_hr_values(fit_bytes, merged_records)

    assert patched == 1
    payload = out_bytes[12:-2]
    assert payload[-1] == 112


def test_render_merged_output_fit_raises_on_written_without_binary_patch():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")
    definition = bytes([
        0x40, 0x00, 0x00, 0x14, 0x00, 0x01, 0xFD, 0x04, 0x86,  # timestamp only, no HR field
    ])
    data_msg = bytes([0x00]) + struct.pack("<I", ts)
    data = definition + data_msg
    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    fit_bytes = header + data + struct.pack("<H", hr_fit_merge._fit_crc(data))

    with pytest.raises(hr_fit_merge.FitHrMergeError, match="patched 0 FIT records"):
        hr_fit_merge.render_merged_output_fit(
            "ride.fit",
            fit_bytes,
            [{"timestamp": "2026-04-11T07:01:05Z", "hr": 155}],
            {"hr_points_written": 1, "hr_points_matched": 1},
        )


def test_render_merged_output_fit_output_crc_is_valid():
    ts = hr_fit_merge._utc_iso_to_fit_seconds("2026-04-11T07:01:05Z")
    definition = bytes([
        0x40,
        0x00,
        0x00,
        0x14, 0x00,
        0x02,
        0xFD, 0x04, 0x86,
        0x03, 0x01, 0x02,
    ])
    data_msg = bytes([0x00]) + struct.pack("<I", ts) + bytes([100])
    data = definition + data_msg
    header = bytes([12, 0x10, 0x00, 0x00]) + struct.pack("<I", len(data)) + b".FIT"
    fit_bytes = header + data + struct.pack("<H", hr_fit_merge._fit_crc(data))

    _filename, out_bytes, report = hr_fit_merge.render_merged_output_fit(
        "ride.fit",
        fit_bytes,
        [{"timestamp": "2026-04-11T07:01:05Z", "hr": 150}],
        {"hr_points_written": 1, "hr_points_matched": 1},
    )
    data_size = int.from_bytes(out_bytes[4:8], "little")
    data_payload = out_bytes[out_bytes[0]:out_bytes[0] + data_size]
    crc_in_file = int.from_bytes(out_bytes[out_bytes[0] + data_size:out_bytes[0] + data_size + 2], "little")
    assert crc_in_file == hr_fit_merge._fit_crc(data_payload)
    assert report["fit_records_patched_in_binary"] == 1
