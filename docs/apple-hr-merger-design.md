# Apple HR + FIT Merger (MVP) — Design Note

## Chosen Apple Health input formats
- **JSON** (Auto Health Export-style arrays/objects with timestamp + value fields).
- **CSV** (timestamp + hr/value columns).
- Auto-detect falls back by checking if payload starts with `{` or `[`.

## Normalization strategy
- Parse source-specific rows into a shared `HrPoint` shape:
  - `timestamp` (UTC `datetime`)
  - `hr` (int bpm)
- Deduplicate by timestamp (latest row wins).
- Filter implausible values by configurable bounds (default 30–240 bpm).
- Keep parser isolated from merge logic.

## Matching strategy
- FIT records are the timeline master.
- Apple HR points are bounded to FIT start/end timestamps.
- Nearest-neighbor per FIT record with configurable tolerance (default ±2s).
- No new FIT record samples are created.

## Why no interpolation in MVP
- Interpolation can hide timestamp alignment issues and may create misleading confidence.
- Nearest-neighbor is deterministic, auditable, and keeps behavior conservative.
- The merge API is cleanly separated, so interpolation can be added later behind an explicit strategy option.

## Why FIT is the master source
- FIT already carries canonical activity structure (timestamps, GPS, distance, cadence/power/speed streams).
- Preserving FIT record topology avoids mutating route/performance telemetry.
- MVP focus is HR enrichment only, so all non-HR fields are left untouched.


## Reusability
- Core merge logic is a pure function (`merge_fit_with_hr`) with no web/CLI coupling.
- Web and CLI adapters both call the same merge engine and parser/fit I/O layers.
