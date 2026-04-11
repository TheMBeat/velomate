from velomate.tools.apple_hr import parse_apple_csv, parse_apple_json


def test_parse_apple_json_success():
    raw = b'[{"timestamp":"2026-04-10T12:01:05Z","hr":142},{"timestamp":"2026-04-10T12:01:06Z","hr":143}]'
    points = parse_apple_json(raw)
    assert len(points) == 2
    assert points[0].hr == 142


def test_parse_apple_csv_success():
    raw = b"timestamp,hr\n2026-04-10T12:01:05Z,142\n2026-04-10T12:01:06Z,143\n"
    points = parse_apple_csv(raw)
    assert len(points) == 2
    assert points[1].hr == 143


def test_normalized_filter_and_duplicates():
    raw = b'[{"timestamp":"2026-04-10T12:01:05Z","hr":25},{"timestamp":"2026-04-10T12:01:05Z","hr":150}]'
    points = parse_apple_json(raw, ignore_implausible=True, min_hr=30, max_hr=240)
    assert len(points) == 1
    assert points[0].hr == 150


def test_parse_auto_health_export_heart_rate_data_avg():
    raw = b'{"heartRateData":[{"date":"2026-04-11 09:01:06 +0200","Avg":126,"Min":126,"Max":126,"units":"bpm"}]}'
    points = parse_apple_json(raw)
    assert len(points) == 1
    assert points[0].hr == 126
    assert points[0].timestamp.isoformat() == "2026-04-11T07:01:06+00:00"
