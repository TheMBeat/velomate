# FIT Import MVP 1 Design Note

## Why REST + upload UI (and not folder-watch)
For MVP 1, the repository already had a long-running ingestor process with DB persistence and metric recalculation. Adding a tiny HTTP surface in the same service gives immediate usability (manual upload page) and automation support (REST) with minimal operational overhead.

Folder-watch was intentionally skipped for now because it adds stateful file lifecycle concerns (partial writes, retries, duplicate processing, cleanup semantics) that are not needed to validate the first import workflow.

## Integration approach
- The ingestor now starts even when `STRAVA_*` credentials are not set.
- Strava polling/backfill is conditional:
  - enabled when credentials exist,
  - skipped when they do not.
- A lightweight built-in Python HTTP server is hosted by the ingestor process:
  - `GET /imports/fit` upload UI
  - `POST /api/imports/fit` parse+preview
  - `POST /api/imports/fit/confirm` persist

## Data flow
1. User uploads `.fit` file.
2. `fitparse` extracts record samples (`record` messages).
3. Parser maps FIT fields into existing `activities` + `activity_streams` shape.
4. Preview payload is kept in short-lived in-memory pending store.
5. Confirm endpoint persists with existing `upsert_activity` + `upsert_streams`.
6. Existing `recalculate_fitness` pipeline is reused so Grafana sees imported rides without a parallel metrics stack.

## Source tagging
Activities now store:
- `source_system` (e.g. `fit_upload`)
- `source_external_id` (SHA-256 of FIT file bytes)
- `source_file_name`

These fields are additive and backward compatible.

Repeated FIT uploads are deduplicated by `(source_system, source_external_id)` so re-importing the same file updates/reuses the existing activity instead of creating duplicates.
