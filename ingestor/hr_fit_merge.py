"""Apple HR + FIT record merger primitives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
import json
from io import BytesIO
import struct

from apple_hr import AppleHrParseError, normalize_hr_series, parse_apple_hr_text_details
from fit_import import FitImportError
from hr_matching import MATCHING_STRATEGIES
from fitparse import FitFile


class FitHrMergeError(ValueError):
    """Raised for invalid merge input or unsupported operations."""


@dataclass(frozen=True)
class MergeOptions:
    tolerance_seconds: int = 2
    overwrite_existing_hr: bool = False
    ignore_implausible_hr: bool = True
    min_hr: int = 30
    max_hr: int = 240
    matching_strategy: str = "nearest"


FIT_EPOCH = datetime(1989, 12, 31, tzinfo=timezone.utc)


def _to_utc_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_iso_to_fit_seconds(ts: str) -> int:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return int((dt - FIT_EPOCH).total_seconds())


def parse_fit_records_for_merge(file_bytes: bytes) -> dict:
    if not file_bytes:
        raise FitImportError("Empty FIT file")

    try:
        fit = FitFile(BytesIO(file_bytes))
        fit.parse()
    except Exception as exc:
        raise FitImportError("Could not parse FIT file") from exc

    records = []
    for msg in fit.get_messages("record"):
        fields = {field.name: field.value for field in msg}
        ts = fields.get("timestamp")
        if ts is None:
            continue
        records.append({"timestamp": _to_utc_iso(ts), "hr": fields.get("heart_rate")})

    if not records:
        raise FitImportError("No FIT record samples found")

    records.sort(key=lambda r: r["timestamp"])
    return {
        "records": records,
        "summary": {
            "start_time": records[0]["timestamp"],
            "end_time": records[-1]["timestamp"],
            "sample_count": len(records),
            "has_existing_hr": any(r.get("hr") is not None for r in records),
        },
    }


def parse_apple_hr_payload(content: bytes, source_type: str = "auto") -> list[dict]:
    return parse_apple_hr_payload_details(content, source_type=source_type)["samples"]


def parse_apple_hr_payload_details(content: bytes, source_type: str = "auto") -> dict:
    text = content.decode("utf-8", errors="replace")
    print(
        "[fit_hr_merge.parse_apple_hr_payload_details] "
        f"source_type_request={source_type}, bytes={len(content)}, decoded_chars={len(text)}"
    )
    try:
        result = parse_apple_hr_text_details(text, source_type=source_type)
    except AppleHrParseError as exc:
        raise FitHrMergeError(str(exc)) from exc
    print(
        "[fit_hr_merge.parse_apple_hr_payload_details] "
        f"detected_source_type={result.get('source_type')}, "
        f"parser_mode={result.get('debug', {}).get('parser_mode')}, "
        f"samples_from_result={len(result.get('samples', []))}"
    )
    return result


def merge_fit_with_hr(fit_records: list[dict], hr_series: list[dict], options: MergeOptions) -> tuple[list[dict], dict]:
    normalized = normalize_hr_series(
        hr_series,
        min_hr=options.min_hr if options.ignore_implausible_hr else -10_000,
        max_hr=options.max_hr if options.ignore_implausible_hr else 10_000,
    )
    if not fit_records:
        raise FitHrMergeError("FIT record set is empty")
    matcher = MATCHING_STRATEGIES.get(options.matching_strategy)
    if matcher is None:
        raise FitHrMergeError(f"Unsupported matching strategy: {options.matching_strategy}")

    fit_times = [datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) for r in fit_records]
    apple = [{"timestamp": datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")), "hr": s["hr"]} for s in normalized]

    start = fit_times[0]
    end = fit_times[-1]
    in_window = [s for s in apple if start <= s["timestamp"] <= end]

    matched = 0
    written = 0
    used_timestamps = []
    merged = []

    for idx, rec in enumerate(fit_records):
        ts = fit_times[idx]
        best = matcher(ts, in_window, options.tolerance_seconds)

        new_rec = dict(rec)
        if best is not None:
            matched += 1
            existing_hr = rec.get("hr")
            if existing_hr is None or options.overwrite_existing_hr:
                new_rec["hr"] = best["hr"]
                written += 1
                used_timestamps.append(best["timestamp"])
        merged.append(new_rec)

    coverage = (written / len(fit_records) * 100.0) if fit_records else 0.0
    gap_count = sum(1 for r in merged if r.get("hr") is None)

    report = {
        "apple_points_total": len(normalized),
        "apple_points_in_fit_window": len(in_window),
        "fit_records_total": len(fit_records),
        "hr_points_matched": matched,
        "hr_points_written": written,
        "coverage_pct": round(coverage, 2),
        "first_hr_timestamp_used": _to_utc_iso(min(used_timestamps)) if used_timestamps else None,
        "last_hr_timestamp_used": _to_utc_iso(max(used_timestamps)) if used_timestamps else None,
        "records_missing_hr_after_merge": gap_count,
        "overwrite_existing_hr": options.overwrite_existing_hr,
    }

    return merged, report


def _fit_crc(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def _decode_timestamp(field_bytes: bytes, little_endian: bool) -> int:
    fmt = "<I" if little_endian else ">I"
    return struct.unpack(fmt, field_bytes)[0]


def _extract_record_layout(defn: dict) -> tuple[int | None, int | None]:
    cursor = 0
    ts_offset = None
    hr_offset = None
    for field_num, size, _base_type in defn["fields"]:
        if field_num == 253 and size == 4:
            ts_offset = cursor
        if field_num == 3 and size >= 1:
            hr_offset = cursor
        cursor += size
    return ts_offset, hr_offset


def rewrite_fit_hr_values(original_fit: bytes, merged_records: list[dict]) -> tuple[bytes, int]:
    """Patch heart_rate bytes for record messages that already include HR field.

    Returns `(new_fit_bytes, patched_count)`.
    """
    if len(original_fit) < 14:
        raise FitHrMergeError("Invalid FIT file")

    header_size = original_fit[0]
    if header_size < 12:
        raise FitHrMergeError("Invalid FIT header")
    data_size = int.from_bytes(original_fit[4:8], "little")
    data_start = header_size
    data_end = data_start + data_size
    if data_end + 2 > len(original_fit):
        raise FitHrMergeError("Corrupt FIT data size")

    data = bytearray(original_fit[data_start:data_end])

    target_by_fit_ts: dict[int, deque[int | None]] = defaultdict(deque)
    for rec in merged_records:
        fit_ts = _utc_iso_to_fit_seconds(rec["timestamp"])
        hr = rec.get("hr")
        target_by_fit_ts[fit_ts].append(None if hr is None else int(hr))

    definitions: dict[int, dict] = {}
    pos = 0
    last_timestamp = None
    patched = 0

    while pos < len(data):
        hdr = data[pos]
        pos += 1

        if hdr & 0x80:  # compressed timestamp data message
            local_msg_type = (hdr >> 5) & 0x03
            time_offset = hdr & 0x1F
            defn = definitions.get(local_msg_type)
            if not defn:
                raise FitHrMergeError("Compressed timestamp before definition")
            if last_timestamp is None:
                raise FitHrMergeError("Compressed timestamp without prior timestamp")
            ts = (last_timestamp & ~0x1F) + time_offset
            if ts < last_timestamp:
                ts += 0x20
            last_timestamp = ts
            msg_len = defn["size"]
            msg_start = pos
            pos += msg_len
            if defn["global_msg_num"] == 20:
                _, hr_offset = _extract_record_layout(defn)
                queued_hr = target_by_fit_ts[ts].popleft() if target_by_fit_ts.get(ts) else None
                if hr_offset is not None and queued_hr is not None:
                    new_hr = max(0, min(255, queued_hr))
                    data[msg_start + hr_offset] = new_hr
                    patched += 1
            continue

        is_definition = bool(hdr & 0x40)
        local_msg_type = hdr & 0x0F

        if is_definition:
            has_dev = bool(hdr & 0x20)
            if pos + 5 > len(data):
                raise FitHrMergeError("Truncated definition message")
            _reserved = data[pos]
            architecture = data[pos + 1]
            little = architecture == 0
            global_msg_num = int.from_bytes(data[pos + 2:pos + 4], "little" if little else "big")
            num_fields = data[pos + 4]
            pos += 5
            fields = []
            total_size = 0
            for _ in range(num_fields):
                if pos + 3 > len(data):
                    raise FitHrMergeError("Truncated field definition")
                field_num = data[pos]
                size = data[pos + 1]
                base_type = data[pos + 2]
                fields.append((field_num, size, base_type))
                total_size += size
                pos += 3
            if has_dev:
                if pos >= len(data):
                    raise FitHrMergeError("Truncated developer field definition")
                num_dev = data[pos]
                pos += 1
                for _ in range(num_dev):
                    if pos + 3 > len(data):
                        raise FitHrMergeError("Truncated developer field definition")
                    _dev_field_num = data[pos]
                    dev_size = data[pos + 1]
                    _dev_data_index = data[pos + 2]
                    total_size += dev_size
                    pos += 3
            definitions[local_msg_type] = {
                "global_msg_num": global_msg_num,
                "fields": fields,
                "size": total_size,
                "little_endian": little,
            }
            continue

        # Normal data message
        defn = definitions.get(local_msg_type)
        if not defn:
            raise FitHrMergeError("Data message before definition")
        msg_len = defn["size"]
        msg_start = pos
        msg_end = pos + msg_len
        if msg_end > len(data):
            raise FitHrMergeError("Truncated data message")

        if defn["global_msg_num"] == 20:
            ts_offset, hr_offset = _extract_record_layout(defn)
            if ts_offset is not None:
                ts_bytes = bytes(data[msg_start + ts_offset: msg_start + ts_offset + 4])
                ts = _decode_timestamp(ts_bytes, defn["little_endian"])
                last_timestamp = ts
                queued_hr = target_by_fit_ts[ts].popleft() if target_by_fit_ts.get(ts) else None
                if hr_offset is not None and queued_hr is not None:
                    new_hr = max(0, min(255, queued_hr))
                    data[msg_start + hr_offset] = new_hr
                    patched += 1

        pos = msg_end

    header = bytearray(original_fit[:header_size])
    out = bytes(header) + bytes(data)
    crc = _fit_crc(out[header_size:])
    out += struct.pack("<H", crc)
    return out, patched


def render_merged_output_fit(original_fit_name: str, original_fit_bytes: bytes, merged_records: list[dict], report: dict) -> tuple[str, bytes, dict]:
    merged_bytes, patched = rewrite_fit_hr_values(original_fit_bytes, merged_records)
    enriched = dict(report)
    enriched["fit_records_patched_in_binary"] = patched
    filename = original_fit_name.rsplit(".", 1)[0] + "_merged_hr.fit"
    return filename, merged_bytes, enriched


def render_merged_output_json(original_fit_name: str, merged_records: list[dict], report: dict) -> bytes:
    payload = {"source_fit": original_fit_name, "merged_records": merged_records, "report": report}
    return json.dumps(payload, indent=2).encode("utf-8")
