"""Per-source runner, frequency (NOT_DUE), unchanged-snapshot skip, dataset status, failure handler."""

import datetime as dt
import json

import pytest

from conftest import REPO_ROOT
from test_runners import FakeResponse, FakeSession, FakeStore, FakeWriter

NOW = dt.datetime(2026, 10, 22, 6, 0)
TODAY = NOW.date()


def _cfg(common, entity, **overrides):
    row = {
        "source_system": "src", "entity": entity, "source_type": "REST", "base_url": "https://x",
        "endpoint": f"/{entity}", "load_type": "full", "landing_path": f"src/{entity}",
    }
    row.update(overrides)
    return common.IngestionConfig.from_dict(row)


def _client(common, responder):
    return common.HttpClient(rate_limit_per_second=1000, session=FakeSession(responder), sleep=lambda s: None)


def _ok(url, params, n):
    return FakeResponse(200, [{"n": n}], url=url)


# --- frequency -------------------------------------------------------------------


@pytest.mark.parametrize("frequency, hours_ago, due", [
    ("daily", None, True),
    ("daily", 23.9, True),     # yesterday's run a few minutes later than today's
    ("daily", 20, False),
    ("weekly", 72, False),
    ("weekly", 160, True),
])
def test_is_due(common, frequency, hours_ago, due):
    cfg = _cfg(common, "a", frequency=frequency)
    last = NOW - dt.timedelta(hours=hours_ago) if hours_ago is not None else None
    assert common.is_due(cfg, last, NOW) is due


def test_invalid_frequency_and_cross_source_fanout_are_rejected(common):
    with pytest.raises(common.ConfigError, match="frequency must be one of"):
        _cfg(common, "a", frequency="hourly")
    with pytest.raises(common.ConfigError, match="same source_system"):
        _cfg(common, "b", request_strategy="fanout", endpoint="/b/{key}", fanout_dataset="other.a", fanout_key_path="id")


# --- source runner ---------------------------------------------------------------


def test_source_runs_fanout_after_its_source_and_filters_by_entity(common):
    fan = _cfg(common, "items", request_strategy="fanout", endpoint="/items/{key}", fanout_dataset="src.parent", fanout_key_path="id")
    parent, other = _cfg(common, "parent"), _cfg(common, "other")
    assert [c.entity for c in common.select_source_datasets([fan, parent, other], "src")] == ["parent", "other", "items"]
    assert [c.entity for c in common.select_source_datasets([fan, parent, other], "src", "other")] == ["other"]


def test_not_due_datasets_are_logged_and_skipped(common):
    daily, weekly = _cfg(common, "daily"), _cfg(common, "weekly", frequency="weekly")
    store = FakeStore(last_success={"src.daily": NOW - dt.timedelta(days=1), "src.weekly": NOW - dt.timedelta(days=2)})
    session = FakeSession(_ok)
    http = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    result = common.run_source("src", [daily, weekly], common.RunContext(pipeline_run_id="r"), store, FakeWriter(), http, now=NOW, today=TODAY)

    assert result["datasets"] == {"src.daily": "SUCCESS", "src.weekly": "NOT_DUE"}
    assert [u for u, _ in session.calls] == ["https://x/daily"]
    not_due = [r for r in store.executions if r["status"] == "NOT_DUE"][0]
    assert not_due["error_message"].startswith("not due: weekly, last success 2026-10-20")


@pytest.mark.parametrize("run_kwargs", [{"mode": "backfill"}, {}])
def test_force_and_backfill_ignore_frequency(common, run_kwargs):
    weekly = _cfg(common, "weekly", frequency="weekly")
    store = FakeStore(last_success={"src.weekly": NOW - dt.timedelta(days=1)})
    force = not run_kwargs
    result = common.run_source("src", [weekly], common.RunContext(**run_kwargs), store, FakeWriter(),
                               _client(common, _ok), force=force, now=NOW, today=TODAY)
    assert result["datasets"] == {"src.weekly": "SUCCESS"}


def test_one_failed_dataset_does_not_stop_the_others(common):
    a, b, c = _cfg(common, "a"), _cfg(common, "b"), _cfg(common, "c")

    def responder(url, params, n):
        return FakeResponse(400, b"bad request") if url.endswith("/b") else _ok(url, params, n)

    store = FakeStore()
    with pytest.raises(common.SourceRunError, match=r"1 of 3 datasets failed in src: src\.b: .*400"):
        common.run_source("src", [a, b, c], common.RunContext(pipeline_run_id="r"), store, FakeWriter(), _client(common, responder), now=NOW, today=TODAY)
    statuses = {r["dataset"]: r["status"] for r in store.executions}
    assert statuses == {"src.a": "SUCCESS", "src.b": "FAILED", "src.c": "SUCCESS"}
    assert {row["dataset"] for row in store.status_rows} == {"src.a", "src.b", "src.c"}  # status refreshed before raising


def test_shared_client_counts_requests_per_dataset(common):
    a, b = _cfg(common, "a"), _cfg(common, "b")
    store = FakeStore()
    common.run_source("src", [a, b], common.RunContext(), store, FakeWriter(), _client(common, _ok), now=NOW, today=TODAY)
    assert [r["api_requests"] for r in store.executions] == [1, 1]


def test_unknown_source_is_a_config_error(common):
    with pytest.raises(common.ConfigError, match="no enabled datasets for source 'nope'"):
        common.run_source("nope", [_cfg(common, "a")], common.RunContext(), FakeStore(), FakeWriter(), now=NOW)


# --- unchanged snapshots -----------------------------------------------------------


def test_unchanged_snapshot_is_skipped_without_writing(common):
    content = json.dumps([{"n": 1}]).encode()
    store = FakeStore(last_sha={"src.a": common.sha256_hex(content)})
    writer = FakeWriter()
    record = common.run_rest_ingestion(_cfg(common, "a"), common.RunContext(), store, writer,
                                       _client(common, lambda u, p, n: FakeResponse(200, content)), today=TODAY)
    assert record["status"] == "SKIPPED" and record["files_written"] == 0 and record["rows_read"] == 1
    assert record["error_message"].startswith("unchanged")
    assert writer.written == {} and store.files == []


def test_changed_snapshot_is_written(common):
    store = FakeStore(last_sha={"src.a": "old-hash"})
    writer = FakeWriter()
    record = common.run_rest_ingestion(_cfg(common, "a"), common.RunContext(pipeline_run_id="r"), store, writer,
                                       _client(common, _ok), today=TODAY)
    assert record["status"] == "SUCCESS" and list(writer.written) == ["Files/src/a/year=2026/ingestion_date=2026-10-22/r.json"]


# --- dataset status ----------------------------------------------------------------


def _hist(status, hours_ago, rows=0, error=None):
    return {"status": status, "end_timestamp": NOW - dt.timedelta(hours=hours_ago), "rows_read": rows, "error_message": error}


def test_status_tracks_last_success_failures_and_deadlines(common):
    cfg = _cfg(common, "a")  # daily: warn after 36h, stale after 72h
    history = [_hist("SUCCESS", 50, 200), _hist("NOT_DUE", 40), _hist("FAILED", 26, error="HTTP 503"), _hist("FAILED", 2, error="HTTP 503")]
    row = common.compute_dataset_status(cfg, history, NOW)
    assert row["last_status"] == "FAILED" and row["consecutive_failures"] == 2 and row["last_error"] == "HTTP 503"
    assert row["last_success_at"] == NOW - dt.timedelta(hours=50)
    assert row["warn_at"] == NOW - dt.timedelta(hours=14) and row["stale_at"] == NOW + dt.timedelta(hours=22)


def test_status_counts_skipped_unchanged_as_fresh(common):
    row = common.compute_dataset_status(_cfg(common, "a"), [_hist("SUCCESS", 48, 81), _hist("SKIPPED", 1, 81)], NOW)
    assert row["last_status"] == "SKIPPED" and row["consecutive_failures"] == 0
    assert row["last_success_at"] == NOW - dt.timedelta(hours=1)


@pytest.mark.parametrize("rows, last, flag", [
    ([200, 210, 220, 205], 0, "LOW"),
    ([200, 210, 220, 205], 5, "LOW"),
    ([200, 210, 220, 205], 190, "OK"),
    ([3, 4, 2], 0, "LOW"),
    ([3, 4, 2], 1, "OK"),        # small snapshots: only zero is suspicious
    ([], 50, "n/a"),
])
def test_volume_flag_on_full_snapshots(common, rows, last, flag):
    history = [_hist("SUCCESS", 100 - i, r) for i, r in enumerate(rows)] + [_hist("SUCCESS", 1, last)]
    assert common.compute_dataset_status(_cfg(common, "a"), history, NOW)["volume_flag"] == flag


def _incremental_cfg(common):
    return _cfg(common, "inc", load_type="incremental", request_strategy="date_window", date_param_start="d1",
                date_param_end="d2", watermark_field="dt", backfill_start="2019-01-01")


def test_volume_flag_not_applied_to_incremental_loads(common):
    # matters in Fabric: backfill 39,333 then daily lookbacks; a quiet day must not be flagged LOW
    history = [_hist("SUCCESS", 100, 39333), _hist("SUCCESS", 50, 217), _hist("SUCCESS", 30, 209), _hist("SUCCESS", 1, 4)]
    row = common.compute_dataset_status(_incremental_cfg(common), history, NOW)
    assert row["volume_flag"] == "n/a" and row["rows_last_load"] == 4 and row["rows_median_recent"] == 217


def test_status_median_ignores_a_one_off_large_load(common):
    history = [_hist("SUCCESS", 100, 39333)] + [_hist("SUCCESS", 90 - i, 210) for i in range(5)] + [_hist("SUCCESS", 1, 200)]
    row = common.compute_dataset_status(_cfg(common, "a"), history, NOW)
    assert row["rows_median_recent"] == 210 and row["volume_flag"] == "OK"


def test_status_schema_matches_rows(common):
    row = common.compute_dataset_status(_cfg(common, "a"), [], NOW)
    ddl = common.CONTROL_TABLE_SCHEMAS[common.TABLE_DATASET_STATUS]
    assert list(row) == [part.strip().split(" ")[0] for part in ddl.split(",")]


# --- sources for the pipeline and the failure handler -------------------------------


def test_enabled_sources(common):
    configs = [_cfg(common, "a"), _cfg(common, "b"), _cfg(common, "c", source_system="other", landing_path="other/c"),
               _cfg(common, "d", source_system="off", landing_path="off/d", enabled=False)]
    assert common.enabled_sources(configs) == [{"source_system": "other", "datasets": 1}, {"source_system": "src", "datasets": 2}]
    assert common.enabled_sources(configs, "src", "b") == [{"source_system": "src", "datasets": 1}]


def test_failure_handler_logs_only_missing_datasets(common):
    a, b = _cfg(common, "a"), _cfg(common, "b")
    store = FakeStore()
    store.executions.append({"dataset": "src.a", "pipeline_run_id": "run-1", "status": "FAILED", "end_timestamp": NOW, "rows_read": 0})
    logged = common.log_pipeline_failure([a, b], store, source_system="src", pipeline_run_id="run-1",
                                         error_message="MagicUsageError", activity_name="Ingest Src", now=NOW)
    assert logged == ["src.b"]
    assert store.executions[-1]["error_message"].startswith("[pipeline activity 'Ingest Src'] MagicUsageError")
    assert [r["dataset"] for r in store.status_rows] == ["src.b"]


def test_failure_handler_without_source_or_config(common):
    store = FakeStore()
    assert common.log_pipeline_failure([], store, pipeline_run_id="r1") == ["__pipeline__"]
    assert common.log_pipeline_failure([], store, source_system="src", pipeline_run_id="r2") == ["src.*"]


# --- pipeline wiring -------------------------------------------------------------------


def test_every_configured_source_has_a_notebook_and_a_pipeline_case(common, config_module):
    sources = {r["source_system"] for r in config_module.INGESTION_CONFIG}
    doc = json.loads((REPO_ROOT / "pipelines/pl_master_ingestion.DataPipeline/pipeline-content.json").read_text(encoding="utf-8"))
    foreach = next(a for a in doc["properties"]["activities"] if a["name"] == "ForEach Source")
    switch = foreach["typeProperties"]["activities"][0]
    cases = {c["value"]: c["activities"][0] for c in switch["typeProperties"]["cases"]}
    assert sources <= set(cases), f"missing pipeline cases for {sorted(sources - set(cases))}"
    assert switch["typeProperties"]["defaultActivities"][0]["type"] == "Fail"
    for source, activity in cases.items():
        platform = REPO_ROOT / f"notebooks/bronze/nb_bronze_{source}.Notebook/.platform"
        assert platform.exists(), f"missing notebook nb_bronze_{source}"
        logical_id = json.loads(platform.read_text(encoding="utf-8"))["config"]["logicalId"]
        assert activity["typeProperties"]["notebookId"] == logical_id
        assert "@pipeline().parameters.force" in json.dumps(activity["typeProperties"]["parameters"])
