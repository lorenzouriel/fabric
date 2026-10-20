# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_framework_common
#
# Shared ingestion framework, loaded by other notebooks with `%run nb_framework_common`.
#
# - Pure-Python parts (config model, date windows, HTTP client, runners) have no Spark
#   or `notebookutils` import at module level, so `tests/` can load this file directly.
# - Spark/Delta and OneLake access live in `ControlStore` and `OneLakeWriter`.

# CELL ********************

import datetime as dt
import hashlib
import io
import json
import os
import tempfile
import time
import traceback
import uuid
import zipfile
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
CONTROL_LAKEHOUSE = "lh_senado_control"
BRONZE_LAKEHOUSE = "lh_senado_bronze"
CONTROL_SCHEMA = "dbo"

TABLE_CONFIG = "etl_ingestion_config"
TABLE_WATERMARK = "etl_watermark"
TABLE_EXECUTION_LOG = "etl_execution_log"
TABLE_LANDED_FILE = "etl_landed_file"

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"

SOURCE_TYPES = {"REST", "FILE"}
LOAD_TYPES = {"full", "incremental"}
REQUEST_STRATEGIES = {"single", "date_window"}
PAGINATION_TYPES = {"none", "page"}
AUTHENTICATION_TYPES = {"none", "api_key_header"}
MODES = {"incremental", "backfill"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Config model
# ---------------------------------------------------------------------------


class ConfigError(ValueError):
    pass


@dataclass
class IngestionConfig:
    source_system: str
    entity: str
    source_type: str
    base_url: str
    endpoint: str
    load_type: str
    landing_path: str
    enabled: bool = True
    description: str = ""
    watermark_field: str = ""
    request_strategy: str = "single"
    request_params: Dict[str, Any] = field(default_factory=dict)
    record_path: str = ""
    date_param_start: str = ""
    date_param_end: str = ""
    window_days: int = 365
    overlap_days: int = 0
    backfill_start: str = ""
    pagination_type: str = "none"
    page_param: str = "pagina"
    max_pages: int = 10000
    authentication_type: str = "none"
    auth_header: str = ""
    secret_name: str = ""
    rate_limit_per_second: float = 5.0

    @property
    def dataset(self) -> str:
        return f"{self.source_system}.{self.entity}"

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "IngestionConfig":
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in row.items() if k in known and v is not None}
        if isinstance(data.get("request_params"), str):
            data["request_params"] = json.loads(data["request_params"] or "{}")
        for name in ("window_days", "overlap_days", "max_pages"):
            if name in data:
                data[name] = int(data[name])
        if "rate_limit_per_second" in data:
            data["rate_limit_per_second"] = float(data["rate_limit_per_second"])
        if "enabled" in data and isinstance(data["enabled"], str):
            data["enabled"] = data["enabled"].strip().lower() in ("true", "1", "yes")
        cfg = cls(**data)
        cfg.validate()
        return cfg

    def to_row(self) -> Dict[str, Any]:
        row = asdict(self)
        row["request_params"] = json.dumps(self.request_params, sort_keys=True)
        return row

    def validate(self) -> None:
        errors = []
        if not self.source_system or not self.entity:
            errors.append("source_system and entity are required")
        if self.source_type not in SOURCE_TYPES:
            errors.append(f"source_type must be one of {sorted(SOURCE_TYPES)}")
        if self.load_type not in LOAD_TYPES:
            errors.append(f"load_type must be one of {sorted(LOAD_TYPES)}")
        if self.request_strategy not in REQUEST_STRATEGIES:
            errors.append(f"request_strategy must be one of {sorted(REQUEST_STRATEGIES)}")
        if self.pagination_type not in PAGINATION_TYPES:
            errors.append(f"pagination_type must be one of {sorted(PAGINATION_TYPES)}")
        if self.authentication_type not in AUTHENTICATION_TYPES:
            errors.append(f"authentication_type must be one of {sorted(AUTHENTICATION_TYPES)}")
        if not self.base_url.startswith("https://"):
            errors.append("base_url must be https")
        if not self.landing_path or self.landing_path.startswith("/") or ".." in self.landing_path:
            errors.append("landing_path must be a relative path without '..'")
        if self.load_type == "incremental":
            if self.request_strategy != "date_window":
                errors.append("incremental loads require request_strategy=date_window")
            if not self.watermark_field:
                errors.append("incremental loads require watermark_field")
            if not self.backfill_start:
                errors.append("incremental loads require backfill_start")
        if self.request_strategy == "date_window":
            if not (self.date_param_start and self.date_param_end):
                errors.append("date_window requires date_param_start and date_param_end")
            if self.window_days < 1:
                errors.append("window_days must be >= 1")
        if self.backfill_start:
            try:
                parse_date(self.backfill_start)
            except ValueError:
                errors.append("backfill_start must be YYYY-MM-DD")
        if self.authentication_type == "api_key_header" and not (self.auth_header and self.secret_name):
            errors.append("api_key_header requires auth_header and secret_name")
        if self.rate_limit_per_second <= 0:
            errors.append("rate_limit_per_second must be > 0")
        if errors:
            raise ConfigError(f"{self.source_system}.{self.entity}: " + "; ".join(errors))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Dates and windows
# ---------------------------------------------------------------------------


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(str(value)[:10])


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def date_windows(start: dt.date, end: dt.date, max_days: int) -> List[Tuple[dt.date, dt.date]]:
    """Split [start, end] into inclusive windows that never cross a calendar year
    and never exceed max_days. Year-aligned windows map 1:1 to Bronze year= partitions."""
    if max_days < 1:
        raise ValueError("max_days must be >= 1")
    windows = []
    cursor = start
    while cursor <= end:
        year_end = dt.date(cursor.year, 12, 31)
        window_end = min(end, year_end, cursor + dt.timedelta(days=max_days - 1))
        windows.append((cursor, window_end))
        cursor = window_end + dt.timedelta(days=1)
    return windows


def resolve_date_range(
    cfg: IngestionConfig,
    mode: str,
    watermark: Optional[str],
    today: dt.date,
    backfill_start: str = "",
    backfill_end: str = "",
) -> Tuple[dt.date, dt.date]:
    """Incremental: from (watermark - overlap_days) or cfg.backfill_start, to today.
    Backfill: explicit range, defaulting to cfg.backfill_start .. today."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {sorted(MODES)}")
    if mode == "backfill":
        start = parse_date(backfill_start or cfg.backfill_start)
        end = parse_date(backfill_end) if backfill_end else today
    elif watermark:
        start = parse_date(watermark) - dt.timedelta(days=cfg.overlap_days)
        end = today
    else:
        start = parse_date(cfg.backfill_start)
        end = today
    return start, min(end, today)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------


def extract_records(payload: Any, record_path: str) -> List[Any]:
    """Follow a dot path (e.g. 'ListaParlamentarEmExercicio.Parlamentares.Parlamentar')
    and return the records as a list. A single object becomes a one-item list."""
    node = payload
    for key in [k for k in (record_path or "").split(".") if k]:
        if not isinstance(node, dict) or key not in node:
            return []
        node = node[key]
    if node is None:
        return []
    return node if isinstance(node, list) else [node]


def max_field_value(records: Iterable[Any], field_name: str) -> Optional[str]:
    values = [
        str(r[field_name])[:10]
        for r in records
        if isinstance(r, dict) and r.get(field_name) not in (None, "")
    ]
    return max(values) if values else None


def greatest(*values: Optional[str]) -> Optional[str]:
    present = [v for v in values if v]
    return max(present) if present else None


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def build_url(base_url: str, endpoint: str) -> str:
    return base_url.rstrip("/") + "/" + endpoint.lstrip("/")


def landing_file_path(landing_path: str, year: int, ingestion_date: dt.date, file_name: str) -> str:
    """Relative Bronze path: Files/<landing_path>/year=YYYY/ingestion_date=YYYY-MM-DD/<file>."""
    return (
        f"Files/{landing_path.strip('/')}/year={year}/"
        f"ingestion_date={ingestion_date.isoformat()}/{file_name}"
    )


def count_csv_rows(content: bytes) -> int:
    lines = content.count(b"\n")
    if content and not content.endswith(b"\n"):
        lines += 1
    return max(lines - 1, 0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# HTTP client: throttling, retries with backoff, deprecation capture
# ---------------------------------------------------------------------------


class HttpError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


@dataclass
class HttpResult:
    url: str
    status: int
    content: bytes
    headers: Dict[str, str]

    def json(self) -> Any:
        return json.loads(self.content.decode("utf-8"))


RETRY_STATUSES = {429, 500, 502, 503, 504}


class HttpClient:
    def __init__(
        self,
        rate_limit_per_second: float = 5.0,
        max_retries: int = 5,
        backoff_seconds: float = 2.0,
        timeout_seconds: float = 120.0,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if session is None:
            import requests

            session = requests.Session()
        self.session = session
        self.min_interval = 1.0 / rate_limit_per_second
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep
        self.clock = clock
        self._last_request = None
        self.api_requests = 0
        self.retry_count = 0
        self.deprecations: Dict[str, str] = {}

    def _throttle(self) -> None:
        if self._last_request is not None:
            wait = self.min_interval - (self.clock() - self._last_request)
            if wait > 0:
                self.sleep(wait)
        self._last_request = self.clock()

    def _retry_delay(self, attempt: int, response: Any) -> float:
        retry_after = None
        if response is not None:
            retry_after = response.headers.get("Retry-After")
        if retry_after and str(retry_after).isdigit():
            return float(retry_after)
        return self.backoff_seconds * (2 ** attempt)

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> HttpResult:
        request_headers = {"Accept": "application/json", "User-Agent": "senado360-ingestion"}
        request_headers.update(headers or {})
        last_error = ""
        for attempt in range(self.max_retries + 1):
            self._throttle()
            self.api_requests += 1
            response = None
            try:
                response = self.session.get(
                    url, params=params, headers=request_headers,
                    timeout=self.timeout_seconds, allow_redirects=True,
                )
            except Exception as exc:  # connection errors, timeouts
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code < 400:
                    resp_headers = {k.lower(): v for k, v in response.headers.items()}
                    if "deprecation" in resp_headers or "sunset" in resp_headers:
                        self.deprecations[url] = (
                            f"deprecation={resp_headers.get('deprecation', '')}; "
                            f"sunset={resp_headers.get('sunset', '')}; "
                            f"successor={resp_headers.get('link', '')}"
                        )
                    return HttpResult(response.url, response.status_code, response.content, resp_headers)
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
                if response.status_code not in RETRY_STATUSES:
                    raise HttpError(f"GET {url} params={params} failed: {last_error}", response.status_code)
            if attempt < self.max_retries:
                self.retry_count += 1
                self.sleep(self._retry_delay(attempt, response))
        raise HttpError(f"GET {url} params={params} failed after {self.max_retries + 1} attempts: {last_error}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Run context and runners (REST / FILE)
# ---------------------------------------------------------------------------


@dataclass
class RunContext:
    pipeline_name: str = "manual"
    pipeline_run_id: str = "manual"
    mode: str = "incremental"
    backfill_start: str = ""
    backfill_end: str = ""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: dt.datetime = field(default_factory=utc_now)


def _auth_headers(cfg: IngestionConfig, secret_provider: Optional[Callable[[str], str]]) -> Dict[str, str]:
    if cfg.authentication_type == "none":
        return {}
    if secret_provider is None:
        raise ConfigError(f"{cfg.dataset}: authentication requires a secret provider")
    return {cfg.auth_header: secret_provider(cfg.secret_name)}


def _execution_record(
    cfg: IngestionConfig, run: RunContext, status: str, *, rows: int = 0, files: int = 0,
    error: str = "", http: Optional[HttpClient] = None,
    watermark_before: Optional[str] = None, watermark_after: Optional[str] = None,
) -> Dict[str, Any]:
    ended = utc_now()
    return {
        "execution_id": run.execution_id,
        "pipeline_name": run.pipeline_name,
        "pipeline_run_id": run.pipeline_run_id,
        "dataset": cfg.dataset,
        "source_system": cfg.source_system,
        "entity": cfg.entity,
        "mode": run.mode,
        "start_timestamp": run.started_at,
        "end_timestamp": ended,
        "duration_seconds": round((ended - run.started_at).total_seconds(), 3),
        "rows_read": rows,
        "rows_written": rows,
        "files_written": files,
        "status": status,
        "error_message": error[:4000] if error else None,
        "api_requests": http.api_requests if http else 0,
        "retry_count": http.retry_count if http else 0,
        "watermark_before": watermark_before,
        "watermark_after": watermark_after,
        "deprecation_notice": "; ".join(f"{u} -> {d}" for u, d in http.deprecations.items())[:4000]
        if http and http.deprecations else None,
    }


PIPELINE_DATASET = "__pipeline__"


def resolve_failure_dataset(config_json: str = "", dataset: str = "") -> str:
    """Dataset key for a pipeline-level failure row. Tolerates a malformed config_json,
    since the failure being logged may be exactly that."""
    if config_json:
        try:
            row = json.loads(config_json)
            if row.get("source_system") and row.get("entity"):
                return f"{row['source_system']}.{row['entity']}"
        except (ValueError, AttributeError):
            pass
    return dataset or PIPELINE_DATASET


def failure_record(
    dataset: str, pipeline_name: str, pipeline_run_id: str, mode: str,
    error_message: str, activity_name: str = "",
) -> Dict[str, Any]:
    """FAILED row written by the pipeline's on-failure handler, for errors raised before
    (or outside) the runner's own try/except: %run, Spark start-up, timeouts, cancels."""
    now = utc_now()
    source_system, _, entity = dataset.partition(".")
    prefix = f"[pipeline activity '{activity_name}'] " if activity_name else "[pipeline] "
    return {
        "execution_id": str(uuid.uuid4()),
        "pipeline_name": pipeline_name,
        "pipeline_run_id": pipeline_run_id,
        "dataset": dataset,
        "source_system": source_system if entity else None,
        "entity": entity or None,
        "mode": mode,
        "start_timestamp": now,
        "end_timestamp": now,
        "duration_seconds": 0.0,
        "rows_read": 0,
        "rows_written": 0,
        "files_written": 0,
        "status": STATUS_FAILED,
        "error_message": (prefix + (error_message or "no error message from pipeline"))[:4000],
        "api_requests": 0,
        "retry_count": 0,
        "watermark_before": None,
        "watermark_after": None,
        "deprecation_notice": None,
    }


def _plan_rest_requests(cfg: IngestionConfig, run: RunContext, watermark: Optional[str], today: dt.date):
    """Yield (year, window_start, window_end, params) for each request to make."""
    base_params = dict(cfg.request_params)
    if cfg.request_strategy == "single":
        return [(today.year, None, None, base_params)]
    start, end = resolve_date_range(cfg, run.mode, watermark, today, run.backfill_start, run.backfill_end)
    plan = []
    for w_start, w_end in date_windows(start, end, cfg.window_days):
        params = dict(base_params)
        params[cfg.date_param_start] = w_start.isoformat()
        params[cfg.date_param_end] = w_end.isoformat()
        plan.append((w_start.year, w_start, w_end, params))
    return plan


def run_rest_ingestion(
    cfg: IngestionConfig,
    run: RunContext,
    store: Any,
    writer: Any,
    http: Optional[HttpClient] = None,
    secret_provider: Optional[Callable[[str], str]] = None,
    today: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Land raw API responses in Bronze, record lineage, then advance the watermark.
    The watermark only moves after every request and file write succeeded."""
    today = today or utc_now().date()
    http = http or HttpClient(rate_limit_per_second=cfg.rate_limit_per_second)
    watermark_before = store.get_watermark(cfg.dataset) if cfg.load_type == "incremental" else None
    try:
        headers = _auth_headers(cfg, secret_provider)
        url = build_url(cfg.base_url, cfg.endpoint)
        plan = _plan_rest_requests(cfg, run, watermark_before, today)
        landed, rows, max_seen = [], 0, None
        for year, w_start, w_end, params in plan:
            pages = range(1, cfg.max_pages + 1) if cfg.pagination_type == "page" else [None]
            for page in pages:
                req_params = dict(params)
                if page is not None:
                    req_params[cfg.page_param] = page
                result = http.get(url, params=req_params, headers=headers)
                records = extract_records(result.json(), cfg.record_path)
                if page is not None and not records:
                    break
                suffix = f"_{w_start.isoformat()}_{w_end.isoformat()}" if w_start else ""
                page_suffix = f"_p{page:05d}" if page is not None else ""
                rel_path = landing_file_path(
                    cfg.landing_path, year, today, f"{run.pipeline_run_id}{suffix}{page_suffix}.json"
                )
                full_path = writer.write_bytes(rel_path, result.content)
                rows += len(records)
                if cfg.watermark_field:
                    max_seen = greatest(max_seen, max_field_value(records, cfg.watermark_field))
                landed.append({
                    "execution_id": run.execution_id,
                    "dataset": cfg.dataset,
                    "file_path": full_path,
                    "source_url": result.url,
                    "request_params": json.dumps(req_params, sort_keys=True),
                    "http_status": result.status,
                    "bytes": len(result.content),
                    "sha256": sha256_hex(result.content),
                    "records": len(records),
                    "window_start": w_start,
                    "window_end": w_end,
                    "landed_at": utc_now(),
                })
        watermark_after = None
        if cfg.load_type == "incremental":
            watermark_after = greatest(watermark_before, max_seen)
            if watermark_after and watermark_after != watermark_before:
                store.commit_watermark(cfg.dataset, "DATE", watermark_after, run.execution_id)
        store.log_landed_files(landed)
        status = STATUS_SUCCESS if plan else STATUS_SKIPPED
        record = _execution_record(
            cfg, run, status, rows=rows, files=len(landed), http=http,
            watermark_before=watermark_before, watermark_after=watermark_after,
        )
        store.log_execution(record)
        return record
    except Exception as exc:
        store.log_execution(_execution_record(
            cfg, run, STATUS_FAILED, error=f"{exc}\n{traceback.format_exc()}", http=http,
            watermark_before=watermark_before, watermark_after=watermark_before,
        ))
        raise


def run_file_ingestion(
    cfg: IngestionConfig,
    run: RunContext,
    store: Any,
    writer: Any,
    http: Optional[HttpClient] = None,
    today: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Download a bulk file, skip it if the same content was already landed, otherwise
    land the original plus (for ZIPs) every extracted member under extracted/."""
    today = today or utc_now().date()
    http = http or HttpClient(rate_limit_per_second=cfg.rate_limit_per_second)
    try:
        url = build_url(cfg.base_url, cfg.endpoint)
        result = http.get(url, params=cfg.request_params or None, headers={"Accept": "*/*"})
        digest = sha256_hex(result.content)
        if store.file_already_landed(cfg.dataset, digest):
            record = _execution_record(cfg, run, STATUS_SKIPPED, http=http, error="unchanged: sha256 already landed")
            store.log_execution(record)
            return record
        file_name = os.path.basename(result.url.split("?")[0]) or f"{cfg.entity}.bin"
        base_name = f"{run.pipeline_run_id}_{file_name}"
        landed, rows = [], 0

        def _land(rel_path: str, content: bytes, records: int) -> None:
            landed.append({
                "execution_id": run.execution_id,
                "dataset": cfg.dataset,
                "file_path": writer.write_bytes(rel_path, content),
                "source_url": result.url,
                "request_params": json.dumps(cfg.request_params, sort_keys=True),
                "http_status": result.status,
                "bytes": len(content),
                "sha256": sha256_hex(content),
                "records": records,
                "window_start": None,
                "window_end": None,
                "landed_at": utc_now(),
            })

        _land(landing_file_path(cfg.landing_path, today.year, today, base_name), result.content, 0)
        if zipfile.is_zipfile(io.BytesIO(result.content)):
            with zipfile.ZipFile(io.BytesIO(result.content)) as archive:
                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    content = archive.read(member)
                    member_rows = count_csv_rows(content) if member.filename.lower().endswith(".csv") else 0
                    rows += member_rows
                    rel = landing_file_path(
                        cfg.landing_path, today.year, today,
                        f"extracted/{run.pipeline_run_id}/{os.path.basename(member.filename)}",
                    )
                    _land(rel, content, member_rows)
        store.log_landed_files(landed)
        record = _execution_record(cfg, run, STATUS_SUCCESS, rows=rows, files=len(landed), http=http)
        store.log_execution(record)
        return record
    except Exception as exc:
        store.log_execution(_execution_record(
            cfg, run, STATUS_FAILED, error=f"{exc}\n{traceback.format_exc()}", http=http,
        ))
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Control-table schemas (Delta, lh_senado_control)
# ---------------------------------------------------------------------------

CONTROL_TABLE_SCHEMAS = {
    TABLE_CONFIG: (
        "source_system STRING, entity STRING, source_type STRING, base_url STRING, endpoint STRING, "
        "load_type STRING, landing_path STRING, enabled BOOLEAN, description STRING, "
        "watermark_field STRING, request_strategy STRING, request_params STRING, record_path STRING, "
        "date_param_start STRING, date_param_end STRING, window_days INT, overlap_days INT, "
        "backfill_start STRING, pagination_type STRING, page_param STRING, max_pages INT, "
        "authentication_type STRING, auth_header STRING, secret_name STRING, "
        "rate_limit_per_second DOUBLE, updated_at TIMESTAMP"
    ),
    TABLE_WATERMARK: (
        "dataset STRING, watermark_type STRING, last_successful_value STRING, "
        "last_successful_execution STRING, updated_at TIMESTAMP"
    ),
    TABLE_EXECUTION_LOG: (
        "execution_id STRING, pipeline_name STRING, pipeline_run_id STRING, dataset STRING, "
        "source_system STRING, entity STRING, mode STRING, start_timestamp TIMESTAMP, "
        "end_timestamp TIMESTAMP, duration_seconds DOUBLE, rows_read BIGINT, rows_written BIGINT, "
        "files_written INT, status STRING, error_message STRING, api_requests INT, retry_count INT, "
        "watermark_before STRING, watermark_after STRING, deprecation_notice STRING"
    ),
    TABLE_LANDED_FILE: (
        "execution_id STRING, dataset STRING, file_path STRING, source_url STRING, "
        "request_params STRING, http_status INT, bytes BIGINT, sha256 STRING, records BIGINT, "
        "window_start DATE, window_end DATE, landed_at TIMESTAMP"
    ),
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Fabric adapters (only used inside Fabric)
# ---------------------------------------------------------------------------


def onelake_path(workspace: str, lakehouse: str, relative: str) -> str:
    return f"abfss://{workspace}@{ONELAKE_HOST}/{lakehouse}.Lakehouse/{relative.lstrip('/')}"


def current_workspace_name() -> str:
    import notebookutils  # noqa: F401  (available in Fabric runtime)

    return notebookutils.runtime.context["currentWorkspaceName"]


class OneLakeWriter:
    """Writes raw bytes to a lakehouse via a local temp file + notebookutils.fs.cp."""

    def __init__(self, workspace: str, lakehouse: str = BRONZE_LAKEHOUSE, fs: Any = None):
        if fs is None:
            import notebookutils

            fs = notebookutils.fs
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.fs = fs

    def write_bytes(self, relative_path: str, content: bytes) -> str:
        target = onelake_path(self.workspace, self.lakehouse, relative_path)
        handle, tmp = tempfile.mkstemp()
        try:
            with os.fdopen(handle, "wb") as fh:
                fh.write(content)
            self.fs.mkdirs(target.rsplit("/", 1)[0])
            self.fs.cp(f"file://{tmp}", target)
        finally:
            os.remove(tmp)
        return target


class ControlStore:
    """Delta-backed control tables in lh_senado_control (Tables/dbo/<table>)."""

    def __init__(self, spark: Any, workspace: str, lakehouse: str = CONTROL_LAKEHOUSE, schema: str = CONTROL_SCHEMA):
        self.spark = spark
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.schema = schema

    def path(self, table: str) -> str:
        return onelake_path(self.workspace, self.lakehouse, f"Tables/{self.schema}/{table}")

    def ensure_tables(self) -> None:
        for table, ddl in CONTROL_TABLE_SCHEMAS.items():
            self.spark.createDataFrame([], ddl).write.format("delta").mode("ignore").save(self.path(table))

    def _append(self, table: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        df = self.spark.createDataFrame(rows, CONTROL_TABLE_SCHEMAS[table])
        self._with_retry(lambda: df.write.format("delta").mode("append").save(self.path(table)))

    @staticmethod
    def _with_retry(action: Callable[[], Any], attempts: int = 6) -> Any:
        for attempt in range(attempts):
            try:
                return action()
            except Exception as exc:  # Delta optimistic-concurrency conflicts
                if "Concurrent" not in type(exc).__name__ and "Concurrent" not in str(exc)[:500]:
                    raise
                if attempt == attempts - 1:
                    raise
                time.sleep(2 * (attempt + 1))

    def read_config(self) -> List[Dict[str, Any]]:
        df = self.spark.read.format("delta").load(self.path(TABLE_CONFIG)).drop("updated_at")
        return [row.asDict() for row in df.collect()]

    def upsert_config(self, configs: List[IngestionConfig]) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        rows = [c.to_row() for c in configs]
        ddl = CONTROL_TABLE_SCHEMAS[TABLE_CONFIG].replace(", updated_at TIMESTAMP", "")
        src = self.spark.createDataFrame(rows, ddl).withColumn("updated_at", F.current_timestamp())
        (
            DeltaTable.forPath(self.spark, self.path(TABLE_CONFIG)).alias("t")
            .merge(src.alias("s"), "t.source_system = s.source_system AND t.entity = s.entity")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(set={"enabled": F.lit(False), "updated_at": F.current_timestamp()})
            .execute()
        )

    def get_watermark(self, dataset: str) -> Optional[str]:
        from pyspark.sql import functions as F

        rows = (
            self.spark.read.format("delta").load(self.path(TABLE_WATERMARK))
            .where(F.col("dataset") == dataset).select("last_successful_value").collect()
        )
        return rows[0][0] if rows else None

    def commit_watermark(self, dataset: str, watermark_type: str, value: str, execution_id: str) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        src = self.spark.createDataFrame(
            [(dataset, watermark_type, value, execution_id)],
            "dataset STRING, watermark_type STRING, last_successful_value STRING, last_successful_execution STRING",
        ).withColumn("updated_at", F.current_timestamp())

        def _merge():
            (
                DeltaTable.forPath(self.spark, self.path(TABLE_WATERMARK)).alias("t")
                .merge(src.alias("s"), "t.dataset = s.dataset")
                # never move a watermark backwards (e.g. after a historical backfill)
                .whenMatchedUpdateAll(condition="s.last_successful_value > t.last_successful_value")
                .whenNotMatchedInsertAll()
                .execute()
            )

        self._with_retry(_merge)

    def log_execution(self, record: Dict[str, Any]) -> None:
        self._append(TABLE_EXECUTION_LOG, [record])

    def log_landed_files(self, records: List[Dict[str, Any]]) -> None:
        self._append(TABLE_LANDED_FILE, records)

    def has_execution(self, pipeline_run_id: str, dataset: str) -> bool:
        from pyspark.sql import functions as F

        return (
            self.spark.read.format("delta").load(self.path(TABLE_EXECUTION_LOG))
            .where((F.col("pipeline_run_id") == pipeline_run_id) & (F.col("dataset") == dataset))
            .limit(1).count() > 0
        )

    def file_already_landed(self, dataset: str, sha256: str) -> bool:
        from pyspark.sql import functions as F

        return (
            self.spark.read.format("delta").load(self.path(TABLE_LANDED_FILE))
            .where((F.col("dataset") == dataset) & (F.col("sha256") == sha256)).limit(1).count() > 0
        )


def key_vault_secret_provider(vault_url: str) -> Callable[[str], str]:
    def _get(secret_name: str) -> str:
        import notebookutils

        return notebookutils.credentials.getSecret(vault_url, secret_name)

    return _get

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
