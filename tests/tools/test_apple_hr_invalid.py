from velomate.tools.apple_hr import parse_apple_hr


def test_invalid_input_returns_empty_series_for_csv_autodetect():
    points = parse_apple_hr(b"not,a,valid,csv", source_type="auto")
    assert points == []
