"""Lookback ('updated in the last N days') and fan-out (one request per key) strategies."""

import datetime as dt
import json

import pytest

from test_runners import FakeResponse, FakeSession, FakeStore, FakeWriter, http_client

D = dt.date
TODAY = D(2026, 10, 20)


def _matters_cfg(common, **overrides):
    row = {
        "source_system": "senado_legislativo", "entity": "matters", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/processo", "load_type": "incremental",
        "request_strategy": "lookback", "lookback_param": "numdias", "lookback_max_days": 30,
        "date_param_start": "dataInicioApresentacao", "date_param_end": "dataFimApresentacao",
        "overlap_days": 1, "watermark_field": "dataUltimaAtualizacao", "backfill_start": "2025-11-01",
        "window_days": 366, "landing_path": "senado_legislativo/matters",
    }
    row.update(overrides)
    return common.IngestionConfig.from_dict(row)


def _affiliations_cfg(common):
    return common.IngestionConfig.from_dict({
        "source_system": "senado_legislativo", "entity": "party_affiliations", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/senador/{key}/filiacoes", "load_type": "full",
        "request_strategy": "fanout", "fanout_dataset": "senado_legislativo.senators_history",
        "fanout_record_path": "L.Parlamentares.Parlamentar", "fanout_key_path": "IdentificacaoParlamentar.CodigoParlamentar",
        "record_path": "F.Parlamentar.Filiacoes.Filiacao", "landing_path": "senado_legislativo/party_affiliations",
    })


class FanoutStore(FakeStore):
    def __init__(self, files=None):
        super().__init__()
        self.files_by_dataset = files or {}

    def latest_landed_files(self, dataset):
        return self.files_by_dataset.get(dataset, [])


class ReadableWriter(FakeWriter):
    def __init__(self, existing):
        super().__init__()
        self.existing = existing

    def read_bytes(self, full_path):
        return self.existing[full_path]


def _history_payload(codes):
    return json.dumps({"L": {"Parlamentares": {"Parlamentar": [{"IdentificacaoParlamentar": {"CodigoParlamentar": c}} for c in codes]}}}).encode()


# --- lookback ------------------------------------------------------------------


def test_lookback_days_uses_watermark_plus_overlap(common):
    cfg = _matters_cfg(common)
    assert common.lookback_days(cfg, "2026-10-20", TODAY) == 1
    assert common.lookback_days(cfg, "2026-10-15", TODAY) == 6
    assert common.lookback_days(cfg, "2026-09-21", TODAY) == 30


def test_lookback_gap_beyond_api_limit_fails_loudly(common):
    with pytest.raises(common.LookbackGapError, match="gap of 31 days.*run mode=backfill"):
        common.lookback_days(_matters_cfg(common), "2026-09-20", TODAY)


def test_lookback_incremental_sends_numdias_only(common):
    session = FakeSession(lambda u, p, n: FakeResponse(200, [{"id": 1, "dataUltimaAtualizacao": "2026-10-20T10:00:00"}]))
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    store = FakeStore(watermark="2026-10-18")
    result = common.run_rest_ingestion(_matters_cfg(common), common.RunContext(pipeline_run_id="r"), store, FakeWriter(), client, today=TODAY)
    assert session.calls == [("https://x/processo", {"numdias": 3})]
    assert store.committed == [("senado_legislativo.matters", "DATE", "2026-10-20")]
    assert result["status"] == "SUCCESS"


def test_lookback_first_run_backfills_by_presentation_windows(common):
    session = FakeSession(lambda u, p, n: FakeResponse(200, [{"id": n, "dataUltimaAtualizacao": "2026-10-19T08:00:00"}]))
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    store = FakeStore(watermark=None)
    common.run_rest_ingestion(_matters_cfg(common), common.RunContext(pipeline_run_id="r"), store, FakeWriter(), client, today=TODAY)
    assert [p for _, p in session.calls] == [
        {"dataInicioApresentacao": "2025-11-01", "dataFimApresentacao": "2025-12-31"},
        {"dataInicioApresentacao": "2026-01-01", "dataFimApresentacao": "2026-10-20"},
    ]
    assert store.committed == [("senado_legislativo.matters", "DATE", "2026-10-19")]


def test_lookback_gap_is_logged_as_failed_without_requests(common):
    session = FakeSession(lambda u, p, n: FakeResponse(200, []))
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    store = FakeStore(watermark="2026-08-01")
    with pytest.raises(common.LookbackGapError):
        common.run_rest_ingestion(_matters_cfg(common), common.RunContext(), store, FakeWriter(), client, today=TODAY)
    assert session.calls == []
    assert store.executions[-1]["status"] == "FAILED" and store.committed == []


def test_lookback_backfill_mode_uses_windows_even_with_watermark(common):
    session = FakeSession(lambda u, p, n: FakeResponse(200, []))
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    run = common.RunContext(mode="backfill", backfill_start="2026-01-01", backfill_end="2026-03-31")
    common.run_rest_ingestion(_matters_cfg(common), run, FakeStore(watermark="2026-10-18"), FakeWriter(), client, today=TODAY)
    assert session.calls == [("https://x/processo", {"dataInicioApresentacao": "2026-01-01", "dataFimApresentacao": "2026-03-31"})]


@pytest.mark.parametrize("overrides, message", [
    ({"load_type": "full"}, "lookback requires load_type=incremental"),
    ({"lookback_param": ""}, "lookback requires lookback_param"),
    ({"date_param_start": ""}, "lookback requires date_param_start"),
])
def test_invalid_lookback_config(common, overrides, message):
    with pytest.raises(common.ConfigError, match=message):
        _matters_cfg(common, **overrides)


# --- fan-out -------------------------------------------------------------------


def test_fanout_requests_each_key_and_lands_one_jsonl(common):
    history_path = "abfss://ws@onelake/lh_senado_bronze.Lakehouse/Files/senado_legislativo/senators_history/x.json"
    store = FanoutStore({"senado_legislativo.senators_history": [history_path]})
    writer = ReadableWriter({history_path: _history_payload(["5672", "22", "5672", "345"])})

    def responder(url, params, n):
        code = url.split("/")[-2]
        return FakeResponse(200, {"F": {"Parlamentar": {"Codigo": code, "Filiacoes": {"Filiacao": [{"p": 1}, {"p": 2}]}}}}, url=url)

    session = FakeSession(responder)
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    result = common.run_rest_ingestion(_affiliations_cfg(common), common.RunContext(pipeline_run_id="r"), store, writer, client, today=TODAY)

    assert [u for u, _ in session.calls] == ["https://x/senador/22/filiacoes", "https://x/senador/345/filiacoes", "https://x/senador/5672/filiacoes"]
    assert result["status"] == "SUCCESS" and result["rows_read"] == 6 and result["files_written"] == 1
    content = writer.written["Files/senado_legislativo/party_affiliations/year=2026/ingestion_date=2026-10-20/r.jsonl"]
    lines = [json.loads(line) for line in content.decode().splitlines()]
    assert [l["key"] for l in lines] == ["22", "345", "5672"]
    assert lines[0]["payload"]["F"]["Parlamentar"]["Codigo"] == "22" and lines[0]["http_status"] == 200
    assert json.loads(store.files[0]["request_params"])["keys"] == 3


def test_fanout_tolerates_404_for_a_key(common):
    path = "p.json"
    store = FanoutStore({"senado_legislativo.senators_history": [path]})
    writer = ReadableWriter({path: _history_payload(["1", "2"])})

    def responder(url, params, n):
        return FakeResponse(404, b"not found") if "/1/" in url else FakeResponse(200, {"F": {"Parlamentar": {"Filiacoes": {"Filiacao": [{}]}}}})

    result = common.run_rest_ingestion(_affiliations_cfg(common), common.RunContext(pipeline_run_id="r"), store, writer, http_client(common, responder), today=TODAY)
    lines = [json.loads(l) for l in next(iter(writer.written.values())).decode().splitlines()]
    assert [(l["key"], l["http_status"]) for l in lines] == [("1", 404), ("2", 200)]
    assert result["rows_read"] == 1


def test_fanout_fails_on_other_errors(common):
    path = "p.json"
    store = FanoutStore({"senado_legislativo.senators_history": [path]})
    writer = ReadableWriter({path: _history_payload(["1"])})
    with pytest.raises(common.HttpError):
        common.run_rest_ingestion(_affiliations_cfg(common), common.RunContext(), store, writer,
                                  http_client(common, lambda u, p, n: FakeResponse(400, b"bad")), today=TODAY)
    assert store.executions[-1]["status"] == "FAILED" and writer.written == {}


def test_fanout_without_source_run_fails_clearly(common):
    with pytest.raises(RuntimeError, match="no successful run of senado_legislativo.senators_history"):
        common.run_rest_ingestion(_affiliations_cfg(common), common.RunContext(), FanoutStore(), ReadableWriter({}),
                                  http_client(common, lambda u, p, n: FakeResponse(200, {})), today=TODAY)


@pytest.mark.parametrize("overrides, message", [
    ({"endpoint": "/senador/filiacoes"}, "must contain"),
    ({"load_type": "incremental", "watermark_field": "x", "backfill_start": "2020-01-01"}, "fanout requires source_type=REST and load_type=full"),
    ({"fanout_key_path": ""}, "fanout requires fanout_dataset and fanout_key_path"),
    ({"request_strategy": "single"}, "only valid with request_strategy=fanout"),
])
def test_invalid_fanout_config(common, overrides, message):
    row = _affiliations_cfg(common).to_row()
    row.update(overrides)
    with pytest.raises(common.ConfigError, match=message):
        common.IngestionConfig.from_dict(row)


# --- yearly ----------------------------------------------------------------------


def _ceaps_cfg(common, **overrides):
    row = {
        "source_system": "senado_admin", "entity": "ceaps", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/senadores/despesas_ceaps/{year}", "load_type": "incremental",
        "request_strategy": "yearly", "refresh_years": 2, "watermark_field": "ano",
        "backfill_start": "2019-01-01", "landing_path": "senado_admin/ceaps",
    }
    row.update(overrides)
    return common.IngestionConfig.from_dict(row)


@pytest.mark.parametrize("mode, watermark, bf_start, bf_end, expected", [
    ("incremental", None, "", "", list(range(2019, 2027))),          # first run: full backfill
    ("incremental", "2026", "", "", [2025, 2026]),                   # steady state: current + previous
    ("incremental", "2023", "", "", [2023, 2024, 2025, 2026]),       # missed runs: extend back to watermark
    ("incremental", "2015", "", "", list(range(2019, 2027))),        # never before configured start
    ("backfill", "2026", "2008-01-01", "2010-12-31", [2008, 2009, 2010]),  # explicit backfill may go earlier
    ("backfill", None, "", "", list(range(2019, 2027))),
])
def test_yearly_range(common, mode, watermark, bf_start, bf_end, expected):
    run = common.RunContext(mode=mode, backfill_start=bf_start, backfill_end=bf_end)
    assert common.yearly_range(_ceaps_cfg(common), run, watermark, TODAY) == expected


def test_yearly_puts_year_in_path_and_commits_year_watermark(common):
    def responder(url, params, n):
        year = int(url.rsplit("/", 1)[1])
        return FakeResponse(200, [{"id": n, "ano": year, "valorReembolsado": 10.0}], url=url)

    session = FakeSession(responder)
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    store, writer = FakeStore(watermark="2026"), FakeWriter()
    result = common.run_rest_ingestion(_ceaps_cfg(common), common.RunContext(pipeline_run_id="r"), store, writer, client, today=TODAY)

    assert session.calls == [
        ("https://x/senadores/despesas_ceaps/2025", {}),
        ("https://x/senadores/despesas_ceaps/2026", {}),
    ]
    assert sorted(writer.written) == [
        "Files/senado_admin/ceaps/year=2025/ingestion_date=2026-10-20/r_2025-01-01_2025-12-31.json",
        "Files/senado_admin/ceaps/year=2026/ingestion_date=2026-10-20/r_2026-01-01_2026-10-20.json",
    ]
    assert result["rows_read"] == 2 and store.committed == []  # watermark already 2026


def test_yearly_first_run_commits_year_watermark(common):
    client = http_client(common, lambda u, p, n: FakeResponse(200, [{"ano": int(u.rsplit("/", 1)[1])}]))
    store = FakeStore()
    common.run_rest_ingestion(_ceaps_cfg(common, backfill_start="2025-01-01"), common.RunContext(), store, FakeWriter(), client, today=TODAY)
    assert store.committed == [("senado_admin.ceaps", "YEAR", "2026")]


@pytest.mark.parametrize("overrides, message", [
    ({"endpoint": "/senadores/despesas_ceaps"}, "must contain '{year}'"),
    ({"refresh_years": 0}, "refresh_years must be >= 1"),
    ({"load_type": "full"}, "yearly requires load_type=incremental"),
])
def test_invalid_yearly_config(common, overrides, message):
    with pytest.raises(common.ConfigError, match=message):
        _ceaps_cfg(common, **overrides)


# --- config + pipeline wiring ----------------------------------------------------


def test_fanout_sources_exist_and_are_not_fanout_themselves(common, config_module):
    configs = {c.dataset: c for c in (common.IngestionConfig.from_dict(r) for r in config_module.INGESTION_CONFIG)}
    for cfg in configs.values():
        if cfg.request_strategy == "fanout":
            source = configs.get(cfg.fanout_dataset)
            assert source is not None, f"{cfg.dataset}: fanout_dataset {cfg.fanout_dataset} not in config"
            assert source.request_strategy != "fanout", "fan-out of fan-out is not ordered by the pipeline"
            assert source.record_path == cfg.fanout_record_path, f"{cfg.dataset}: fanout_record_path should match the source's record_path"
            assert source.source_system == cfg.source_system, "fan-out source must be in the same source notebook"
