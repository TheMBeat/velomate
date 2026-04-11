from __future__ import annotations

from datetime import timezone
from io import BytesIO

from velomate.tools.hr_models import FitRecord


class FitIoError(RuntimeError):
    pass


def _load_fit_tool():
    try:
        from fit_tool.fit_file import FitFile  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency guard
        raise FitIoError("FIT support requires optional dependency 'fit-tool'.") from exc
    return FitFile


def parse_fit_records(raw: bytes) -> tuple[list[FitRecord], object]:
    FitFile = _load_fit_tool()
    fit_file = FitFile.from_bytes(raw)
    records: list[FitRecord] = []
    for msg in fit_file.messages:
        if msg.name != "record":
            continue
        values = {f.name: f.value for f in msg.fields}
        ts = values.get("timestamp")
        if ts is None:
            continue
        ts = ts.astimezone(timezone.utc)
        hr = values.get("heart_rate")
        records.append(FitRecord(timestamp=ts, heart_rate=int(hr) if hr is not None else None))
    return records, fit_file


def write_fit_with_hr(fit_file: object, merged_records: list[FitRecord]) -> bytes:
    idx = 0
    for msg in fit_file.messages:  # type: ignore[attr-defined]
        if msg.name != "record":
            continue
        if idx >= len(merged_records):
            break
        new_hr = merged_records[idx].heart_rate
        if new_hr is not None:
            for field in msg.fields:
                if field.name == "heart_rate":
                    field.value = new_hr
                    break
            else:
                msg.set_value("heart_rate", new_hr)
        idx += 1

    out = BytesIO()
    fit_file.to_file(out)
    return out.getvalue()
