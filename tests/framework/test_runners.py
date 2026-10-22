import datetime as dt
import io
import json
import zipfile

import pytest


# --- fakes -------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status, body=b"", headers=None, url="https://x"):
        self.status_code = status
        self.content = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.headers = headers or {}
        self.url = url
        self.text = self.content.decode("utf-8", "replace")


class FakeSession:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, allow_redirects=True):
        self.calls.append((url, dict(params or {})))
        return self.responder(url, params or {}, len(self.calls))


class FakeStore:
    def __init__(self, watermark=None, landed_hashes=(), last_sha=None, last_success=None):
        self.watermark = watermark
        self.committed = []
        self.executions = []
        self.files = []
        self.landed_hashes = set(landed_hashes)
        self.last_sha = last_sha or {}
        self.last_success = last_success or {}
        self.status_rows = []

    def last_landed_sha(self, dataset):
        return self.last_sha.get(dataset)

    def last_success_at(self, datasets):
        return {d: t for d, t in self.last_success.items() if d in datasets}

    def execution_history(self, datasets):
        history = {}
        for r in self.executions:
            if r["dataset"] in datasets and "end_timestamp" in r:
                history.setdefault(r["dataset"], []).append(r)
        return history

    def upsert_dataset_status(self, rows):
        self.status_rows = rows

    def has_execution(self, pipeline_run_id, dataset):
        return any(r.get("pipeline_run_id") == pipeline_run_id and r["dataset"] == dataset for r in self.executions)

    def get_watermark(self, dataset):
        return self.watermark

    def commit_watermark(self, dataset, wtype, value, execution_id):
        self.committed.append((dataset, wtype, value))

    def log_execution(self, record):
        self.executions.append(record)

    def log_landed_files(self, records):
        self.files.extend(records)

    def file_already_landed(self, dataset, sha):
        return sha in self.landed_hashes


class FakeWriter:
    def __init__(self, fail_on=None):
        self.written = {}
        self.fail_on = fail_on

    def write_bytes(self, rel, content):
        if self.fail_on and self.fail_on in rel:
            raise IOError("disk full")
        self.written[rel] = content
        return f"abfss://ws@onelake/lh_senado_bronze.Lakehouse/{rel}"


def http_client(common, responder):
    return common.HttpClient(rate_limit_per_second=1000, backoff_seconds=0, session=FakeSession(responder), sleep=lambda s: None)


# --- HTTP client ---------------------------------------------------------------


def test_http_retries_429_then_succeeds_and_honours_retry_after(common):
    sleeps = []
    responses = [FakeResponse(429, b"slow down", {"Retry-After": "3"}), FakeResponse(200, [{"a": 1}])]
    client = common.HttpClient(rate_limit_per_second=1000, session=FakeSession(lambda u, p, n: responses[n - 1]), sleep=sleeps.append)
    result = client.get("https://x/votacao")
    assert result.json() == [{"a": 1}]
    assert client.api_requests == 2 and client.retry_count == 1
    assert 3.0 in sleeps


def test_http_client_error_is_not_retried(common):
    client = http_client(common, lambda u, p, n: FakeResponse(400, b"Limite o periodo a 1 ano"))
    with pytest.raises(common.HttpError, match="400"):
        client.get("https://x/votacao")
    assert client.api_requests == 1


def test_http_gives_up_after_max_retries(common):
    client = http_client(common, lambda u, p, n: FakeResponse(503, b"busy"))
    client.max_retries = 2
    with pytest.raises(common.HttpError, match="after 3 attempts"):
        client.get("https://x")
    assert client.api_requests == 3


def test_http_captures_deprecation_headers(common):
    headers = {"Deprecation": "Tue, 18 Mar 2025", "Sunset": "Sun, 01 Feb 2026", "Link": "<https://x/votacao>; rel=successor"}
    client = http_client(common, lambda u, p, n: FakeResponse(200, [], headers))
    client.get("https://x/old")
    assert "sunset=Sun, 01 Feb 2026" in client.deprecations["https://x/old"]


# --- REST runner ---------------------------------------------------------------


def _votes_cfg(common):
    return common.IngestionConfig.from_dict({
        "source_system": "senado_legislativo", "entity": "votes", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/votacao", "load_type": "incremental",
        "request_strategy": "date_window", "date_param_start": "dataInicio", "date_param_end": "dataFim",
        "watermark_field": "dataSessao", "backfill_start": "2025-11-01", "overlap_days": 7,
        "landing_path": "senado_legislativo/votes",
    })


def _votes_responder(u, params, n):
    year = params["dataInicio"][:4]
    return FakeResponse(200, [{"codigoSessaoVotacao": n, "dataSessao": f"{year}-12-01" if year == "2025" else "2026-10-14", "votos": []}])


def test_first_incremental_run_backfills_by_year_and_commits_watermark(common):
    store, writer = FakeStore(), FakeWriter()
    run = common.RunContext(pipeline_run_id="run1")
    result = common.run_rest_ingestion(_votes_cfg(common), run, store, writer, http_client(common, _votes_responder), today=dt.date(2026, 10, 19))

    assert result["status"] == "SUCCESS"
    assert result["rows_read"] == 2 and result["files_written"] == 2
    assert sorted(writer.written) == [
        "Files/senado_legislativo/votes/year=2025/ingestion_date=2026-10-19/run1_2025-11-01_2025-12-31.json",
        "Files/senado_legislativo/votes/year=2026/ingestion_date=2026-10-19/run1_2026-01-01_2026-10-19.json",
    ]
    assert store.committed == [("senado_legislativo.votes", "DATE", "2026-10-14")]
    assert result["watermark_before"] is None and result["watermark_after"] == "2026-10-14"
    assert len(store.files) == 2 and store.files[0]["sha256"]


def test_incremental_run_requests_overlap_window(common):
    store, writer = FakeStore(watermark="2026-10-14"), FakeWriter()
    session = FakeSession(_votes_responder)
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    common.run_rest_ingestion(_votes_cfg(common), common.RunContext(), store, writer, client, today=dt.date(2026, 10, 19))
    assert session.calls == [("https://x/votacao", {"dataInicio": "2026-10-07", "dataFim": "2026-10-19"})]
    assert store.committed == []  # max(dataSessao) == existing watermark, nothing to advance


def test_failure_logs_failed_and_never_advances_watermark(common):
    store = FakeStore(watermark="2025-11-20")
    writer = FakeWriter(fail_on="year=2026")
    with pytest.raises(IOError):
        common.run_rest_ingestion(_votes_cfg(common), common.RunContext(), store, writer, http_client(common, _votes_responder), today=dt.date(2026, 10, 19))
    assert store.committed == []
    assert store.files == []
    assert store.executions[-1]["status"] == "FAILED"
    assert "disk full" in store.executions[-1]["error_message"]
    assert store.executions[-1]["watermark_after"] == "2025-11-20"


def test_full_snapshot_single_request(common):
    cfg = common.IngestionConfig.from_dict({
        "source_system": "senado_legislativo", "entity": "senators", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/senador/lista/atual", "load_type": "full",
        "record_path": "L.Parlamentares.Parlamentar", "landing_path": "senado_legislativo/senators",
    })
    payload = {"L": {"Parlamentares": {"Parlamentar": [{"id": 1}, {"id": 2}, {"id": 3}]}}}
    store, writer = FakeStore(), FakeWriter()
    result = common.run_rest_ingestion(cfg, common.RunContext(pipeline_run_id="r"), store, writer,
                                       http_client(common, lambda u, p, n: FakeResponse(200, payload)), today=dt.date(2026, 10, 19))
    assert result["rows_read"] == 3 and result["files_written"] == 1
    assert list(writer.written) == ["Files/senado_legislativo/senators/year=2026/ingestion_date=2026-10-19/r.json"]
    assert store.committed == []


def test_page_pagination_stops_on_empty_page(common):
    cfg = common.IngestionConfig.from_dict({
        "source_system": "transparencia", "entity": "amendments_api", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/emendas", "load_type": "full", "pagination_type": "page",
        "request_params": {"ano": 2026}, "landing_path": "transparencia/amendments_api",
    })
    pages = {1: [{"a": 1}], 2: [{"a": 2}], 3: []}
    store, writer = FakeStore(), FakeWriter()
    result = common.run_rest_ingestion(cfg, common.RunContext(pipeline_run_id="r"), store, writer,
                                       http_client(common, lambda u, p, n: FakeResponse(200, pages[p["pagina"]])), today=dt.date(2026, 10, 19))
    assert result["rows_read"] == 2 and result["files_written"] == 2 and result["api_requests"] == 3


def test_api_key_requires_secret_provider(common):
    cfg = common.IngestionConfig.from_dict({
        "source_system": "transparencia", "entity": "amendments_api", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/emendas", "load_type": "full",
        "authentication_type": "api_key_header", "auth_header": "chave-api-dados", "secret_name": "transparencia-api-key",
        "landing_path": "transparencia/amendments_api",
    })
    seen = {}

    def responder(u, p, n):
        return FakeResponse(200, [])

    session = FakeSession(responder)
    original_get = session.get

    def capturing_get(url, params=None, headers=None, **kw):
        seen.update(headers or {})
        return original_get(url, params=params, headers=headers, **kw)

    session.get = capturing_get
    client = common.HttpClient(rate_limit_per_second=1000, session=session, sleep=lambda s: None)
    common.run_rest_ingestion(cfg, common.RunContext(), FakeStore(), FakeWriter(), client,
                              secret_provider=lambda name: f"secret-for-{name}", today=dt.date(2026, 10, 19))
    assert seen["chave-api-dados"] == "secret-for-transparencia-api-key"


# --- FILE runner ---------------------------------------------------------------


def _zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("EmendasParlamentares.csv", "Código;Valor\n1;10,00\n2;20,00\n".encode("latin-1"))
        zf.writestr("EmendasParlamentares_Convenios.csv", "Código;Valor\n1;5,00\n".encode("latin-1"))
    return buf.getvalue()


def _file_cfg(common):
    return common.IngestionConfig.from_dict({
        "source_system": "transparencia", "entity": "amendments", "source_type": "FILE",
        "base_url": "https://portaldatransparencia.gov.br", "endpoint": "/download-de-dados/emendas-parlamentares/UNICO",
        "load_type": "full", "landing_path": "transparencia/amendments",
    })


def test_file_ingestion_lands_zip_and_members_unchanged(common):
    content = _zip_bytes()
    store, writer = FakeStore(), FakeWriter()
    client = http_client(common, lambda u, p, n: FakeResponse(200, content, url="https://cdn/x/EmendasParlamentares.zip"))
    result = common.run_file_ingestion(_file_cfg(common), common.RunContext(pipeline_run_id="r"), store, writer, client, today=dt.date(2026, 10, 19))

    base = "Files/transparencia/amendments/year=2026/ingestion_date=2026-10-19"
    assert result["status"] == "SUCCESS" and result["rows_read"] == 3 and result["files_written"] == 3
    assert writer.written[f"{base}/r_EmendasParlamentares.zip"] == content
    member = writer.written[f"{base}/extracted/r/EmendasParlamentares.csv"]
    assert member.decode("latin-1").startswith("Código")  # bytes preserved, still Latin-1


def test_file_ingestion_skips_unchanged_content(common):
    content = _zip_bytes()
    store = FakeStore(landed_hashes={common.sha256_hex(content)})
    writer = FakeWriter()
    client = http_client(common, lambda u, p, n: FakeResponse(200, content))
    result = common.run_file_ingestion(_file_cfg(common), common.RunContext(), store, writer, client, today=dt.date(2026, 10, 19))
    assert result["status"] == "SKIPPED"
    assert writer.written == {}
