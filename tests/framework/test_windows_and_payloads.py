import datetime as dt

D = dt.date


def test_windows_split_on_year_boundary_and_max_days(common):
    windows = common.date_windows(D(2025, 11, 1), D(2026, 3, 1), 365)
    assert windows == [(D(2025, 11, 1), D(2025, 12, 31)), (D(2026, 1, 1), D(2026, 3, 1))]

    windows = common.date_windows(D(2026, 1, 1), D(2026, 1, 10), 4)
    assert windows == [(D(2026, 1, 1), D(2026, 1, 4)), (D(2026, 1, 5), D(2026, 1, 8)), (D(2026, 1, 9), D(2026, 1, 10))]


def test_windows_never_exceed_api_limit_over_full_backfill(common):
    windows = common.date_windows(D(2019, 2, 1), D(2026, 10, 19), 366)
    assert len(windows) == 8  # one per calendar year 2019..2026, leap years included
    assert all((end - start).days < 366 for start, end in windows)
    assert all(start.year == end.year for start, end in windows)


def test_empty_range_yields_no_windows(common):
    assert common.date_windows(D(2026, 10, 20), D(2026, 10, 19), 365) == []


def _votes_cfg(common):
    return common.IngestionConfig.from_dict({
        "source_system": "senado_legislativo", "entity": "votes", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/votacao", "load_type": "incremental",
        "request_strategy": "date_window", "date_param_start": "dataInicio", "date_param_end": "dataFim",
        "watermark_field": "dataSessao", "backfill_start": "2019-02-01", "overlap_days": 7,
        "landing_path": "senado_legislativo/votes",
    })


def test_incremental_range_uses_watermark_minus_overlap(common):
    cfg = _votes_cfg(common)
    assert common.resolve_date_range(cfg, "incremental", "2026-10-18", D(2026, 10, 19)) == (D(2026, 10, 11), D(2026, 10, 19))


def test_first_incremental_run_starts_at_backfill_start(common):
    cfg = _votes_cfg(common)
    assert common.resolve_date_range(cfg, "incremental", None, D(2026, 10, 19)) == (D(2019, 2, 1), D(2026, 10, 19))


def test_backfill_ignores_watermark_and_caps_at_today(common):
    cfg = _votes_cfg(common)
    rng = common.resolve_date_range(cfg, "backfill", "2026-10-18", D(2026, 10, 19), "2022-01-01", "2030-01-01")
    assert rng == (D(2022, 1, 1), D(2026, 10, 19))


def test_extract_records_paths(common):
    payload = {"A": {"B": {"C": [{"x": 1}, {"x": 2}]}}}
    assert common.extract_records(payload, "A.B.C") == [{"x": 1}, {"x": 2}]
    assert common.extract_records({"A": {"B": {"C": {"x": 1}}}}, "A.B.C") == [{"x": 1}]
    assert common.extract_records(payload, "A.missing") == []
    assert common.extract_records([{"x": 1}], "") == [{"x": 1}]


def test_max_field_value_and_greatest(common):
    records = [{"dataSessao": "2026-10-14"}, {"dataSessao": "2026-10-15T10:00"}, {"dataSessao": None}]
    assert common.max_field_value(records, "dataSessao") == "2026-10-15"
    assert common.greatest(None, "2026-01-01", "2025-12-31") == "2026-01-01"
    assert common.greatest(None, None) is None


def test_landing_file_path(common):
    path = common.landing_file_path("senado_legislativo/votes", 2026, D(2026, 10, 19), "run1.json")
    assert path == "Files/senado_legislativo/votes/year=2026/ingestion_date=2026-10-19/run1.json"


def test_count_csv_rows(common):
    assert common.count_csv_rows(b"h1;h2\n1;2\n3;4\n") == 2
    assert common.count_csv_rows(b"h1;h2\n1;2") == 1
    assert common.count_csv_rows(b"") == 0
