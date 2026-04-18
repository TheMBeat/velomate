"""Apple HR + FIT record merger primitives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict, deque
from io import BytesIO
import json
import struct

from apple_hr import AppleHrParseError, normalize_hr_series, parse_apple_hr_text_details
from fit_import import FitImportError
from fitparse import FitFile


class FitHrMergeError(ValueError):
    """Raised for invalid merge input or unsupported operations."""


@dataclass(frozen=True)
class MergeOptions:
    overwrite_existing_hr: bool = False
    ignore_implausible_hr: bool = True
    min_hr: int = 30
    max_hr: int = 240


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


def parse_apple_hr_payload_details(
    content: bytes,
    source_type: str = "auto",
    *,
    fit_start_time: str | None = None,
    fit_end_time: str | None = None,
) -> dict:
    text = content.decode("utf-8-sig", errors="replace")
    fit_start = datetime.fromisoformat(fit_start_time.replace("Z", "+00:00")) if fit_start_time else None
    fit_end = datetime.fromisoformat(fit_end_time.replace("Z", "+00:00")) if fit_end_time else None
    try:
        result = parse_apple_hr_text_details(text, source_type=source_type, fit_start=fit_start, fit_end=fit_end)
    except AppleHrParseError as exc:
        raise FitHrMergeError(str(exc)) from exc
    return result


def interpolate_hr(samples: list[dict], fit_timestamps: list[str]) -> list[int | None]:
    """Map sparse Apple HR samples to FIT timestamps using linear interpolation.

    Returns a list aligned to `fit_timestamps` containing int HR values or None.
    Interpolation is only performed inside the Apple HR sample time range.
    """
    if not fit_timestamps:
        return []
    if not samples:
        return [None] * len(fit_timestamps)

    parsed = []
    for item in samples:
        ts = item.get("timestamp")
        hr = item.get("hr")
        if ts in (None, "") or hr is None:
            continue
        parsed.append(
            {
                "timestamp": datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc),
                "hr": int(hr),
            }
        )
    parsed.sort(key=lambda x: x["timestamp"])
    if not parsed:
        return [None] * len(fit_timestamps)

    out: list[int | None] = []
    first_ts = parsed[0]["timestamp"]
    last_ts = parsed[-1]["timestamp"]
    right = 0

    for fit_ts_str in fit_timestamps:
        fit_ts = datetime.fromisoformat(fit_ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        if fit_ts < first_ts or fit_ts > last_ts:
            out.append(None)
            continue

        while right < len(parsed) and parsed[right]["timestamp"] < fit_ts:
            right += 1

        if right < len(parsed) and parsed[right]["timestamp"] == fit_ts:
            out.append(parsed[right]["hr"])
            continue
        if right == 0 or right >= len(parsed):
            out.append(None)
            continue

        left_point = parsed[right - 1]
        right_point = parsed[right]
        left_ts = left_point["timestamp"]
        right_ts = right_point["timestamp"]
        total_seconds = (right_ts - left_ts).total_seconds()
        if total_seconds <= 0:
            out.append(left_point["hr"])
            continue
        ratio = (fit_ts - left_ts).total_seconds() / total_seconds
        hr_value = left_point["hr"] + (right_point["hr"] - left_point["hr"]) * ratio
        out.append(int(round(hr_value)))
    return out


def merge_fit_with_hr(fit_records: list[dict], hr_series: list[dict], options: MergeOptions) -> tuple[list[dict], dict]:
    normalized = normalize_hr_series(
        hr_series,
        min_hr=options.min_hr if options.ignore_implausible_hr else -10_000,
        max_hr=options.max_hr if options.ignore_implausible_hr else 10_000,
    )
    if not fit_records:
        raise FitHrMergeError("FIT record set is empty")
    fit_timestamps = [r["timestamp"] for r in fit_records]
    interpolated = interpolate_hr(normalized, fit_timestamps)

    interpolated_count = sum(1 for value in interpolated if value is not None)
    written = 0
    used_timestamps: list[datetime] = []
    merged = []

    for idx, rec in enumerate(fit_records):
        new_hr = interpolated[idx]
        new_rec = dict(rec)
        if new_hr is not None:
            existing_hr = rec.get("hr")
            if existing_hr is None or options.overwrite_existing_hr:
                new_rec["hr"] = new_hr
                written += 1
                used_timestamps.append(datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc))
        merged.append(new_rec)

    coverage = (written / len(fit_records) * 100.0) if fit_records else 0.0
    gap_count = sum(1 for r in merged if r.get("hr") is None)

    report = {
        "apple_points_total": len(normalized),
        "apple_points_in_fit_window": interpolated_count,
        "fit_records_total": len(fit_records),
        "hr_points_matched": interpolated_count,
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


def _validate_fit_crc(fit_bytes: bytes) -> None:
    if len(fit_bytes) < 14:
        raise FitHrMergeError("Invalid FIT file")
    header_size = fit_bytes[0]
    if header_size < 12:
        raise FitHrMergeError("Invalid FIT header")
    data_size = int.from_bytes(fit_bytes[4:8], "little")
    data_start = header_size
    data_end = data_start + data_size
    if data_end + 2 > len(fit_bytes):
        raise FitHrMergeError("Corrupt FIT data size")

    expected_crc = int.from_bytes(fit_bytes[data_end:data_end + 2], "little")
    actual_crc = _fit_crc(fit_bytes[data_start:data_end])
    if actual_crc != expected_crc:
        raise FitHrMergeError("Invalid file CRC after merge")


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
    _validate_fit_crc(merged_bytes)
    enriched = dict(report)
    enriched["fit_records_patched_in_binary"] = patched
    if enriched.get("hr_points_written", 0) > 0 and patched == 0:
        raise FitHrMergeError(
            "Merge wrote HR values in memory but patched 0 FIT records in binary. "
            "Input FIT likely has no writable heart_rate field in record messages."
        )
    if patched > enriched.get("hr_points_written", 0):
        raise FitHrMergeError("Binary patched count exceeds planned HR writes")
    filename = original_fit_name.rsplit(".", 1)[0] + "_merged_hr.fit"
    return filename, merged_bytes, enriched


def render_merged_output_json(original_fit_name: str, merged_records: list[dict], report: dict) -> bytes:
    payload = {"source_fit": original_fit_name, "merged_records": merged_records, "report": report}
    return json.dumps(payload, indent=2).encode("utf-8")
