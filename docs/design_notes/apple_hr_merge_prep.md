# Apple HR + FIT merger note

This iteration adds an initial Apple HR + FIT merger surface and keeps merge logic isolated for future FIT-binary writer integration.

## Apple Health formats chosen

Supported input sources:
- Auto Health Export style JSON with `heartRateData` entries (`date`, `Avg`, `Min`, `Max`, `units`)
- Generic list/object JSON samples with `timestamp` + `hr`
- CSV with timestamp + HR columns

For Auto Health Export workout samples, `Avg` is used as the HR value.

## Normalization strategy

1. Parse source payload (JSON/CSV/auto-detect).
2. Convert to normalized series shape:
   - `{"timestamp": "...Z", "hr": 142}`
3. Normalize timestamps to UTC.
4. Keep HR optional/sparse; filter implausible values when enabled.
5. Deduplicate duplicate timestamps (last sample wins).

## Matching strategy

- FIT is the master timeline.
- Nearest-neighbor timestamp mapping with configurable tolerance (default ±2s).
- Only points within FIT start/end are considered.
- No interpolation in MVP.

## Why no interpolation in MVP

- Avoids inventing physiological signal where there is no sampled value.
- Keeps behavior conservative and auditable.
- Easier to validate with quality metrics before introducing interpolation policy.

## Why FIT remains master source

- Preserves route, distance, cadence, power, speed, and sample structure exactly.
- Prevents accidental creation of synthetic timeline points.
- Keeps merge as a bounded HR enrichment step for one workout.

## Current output format

- The merger web tool emits a new FIT file (`*_merged_hr.fit`) and exposes merge report stats in the API/UI response.
- Binary patching is conservative: existing FIT fields/structure are preserved and only HR field bytes are updated where record messages already contain HR.
