from datetime import datetime, timezone

from velomate.tools.fit_io import write_fit_with_hr
from velomate.tools.hr_models import FitRecord


class _Field:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class _Msg:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.set_values = {}

    def set_value(self, key, value):
        self.set_values[key] = value
        self.fields.append(_Field(key, value))


class _FitFile:
    def __init__(self, messages):
        self.messages = messages

    def to_file(self, out):
        out.write(b"ok")


def test_write_skips_timestamp_less_records_to_keep_index_alignment():
    t0 = datetime(2026, 4, 11, 7, 1, 6, tzinfo=timezone.utc)
    t1 = datetime(2026, 4, 11, 7, 1, 8, tzinfo=timezone.utc)

    msg1 = _Msg("record", [_Field("timestamp", t0), _Field("heart_rate", 100)])
    msg2 = _Msg("record", [_Field("heart_rate", 101)])  # no timestamp, skipped by parser
    msg3 = _Msg("record", [_Field("timestamp", t1), _Field("heart_rate", 102)])

    fit_obj = _FitFile([msg1, msg2, msg3])
    merged = [FitRecord(timestamp=t0, heart_rate=140), FitRecord(timestamp=t1, heart_rate=150)]

    write_fit_with_hr(fit_obj, merged)

    assert msg1.fields[1].value == 140
    assert msg3.fields[1].value == 150
    assert msg2.fields[0].value == 101
