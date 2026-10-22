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
TABLE_DATASET_STATUS = "etl_dataset_status"

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"      # ran, nothing new (no window, or snapshot identical to the last one)
STATUS_NOT_DUE = "NOT_DUE"      # not run: its frequency says it is not due yet
SUCCESSFUL_STATUSES = {STATUS_SUCCESS, STATUS_SKIPPED}

# frequency -> hours between runs; a dataset is due after 90% of the interval, so a daily
# run at 06:00 still picks up yesterday's 06:05 run.
FREQUENCY_HOURS = {"daily": 24, "weekly": 24 * 7, "monthly": 24 * 30}
DUE_TOLERANCE = 0.9
WARN_FACTOR, STALE_FACTOR = 1.5, 3.0

SOURCE_TYPES = {"REST", "FILE"}
LOAD_TYPES = {"full", "incremental"}
REQUEST_STRATEGIES = {"single", "date_window", "lookback", "fanout", "yearly"}
INCREMENTAL_STRATEGIES = {"date_window", "lookback", "yearly"}
DATE_PARAM_STRATEGIES = {"date_window", "lookback"}
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
    # lookback: incremental runs send "<lookback_param>=<days since watermark>"; first run and
    # backfills fall back to date windows on date_param_start/date_param_end.
    lookback_param: str = ""
    lookback_max_days: int = 30
    # fanout: one request per key; keys come from the latest successful Bronze files of
    # fanout_dataset (records at fanout_record_path, key at fanout_key_path). Endpoint has "{key}".
    fanout_dataset: str = ""
    fanout_record_path: str = ""
    fanout_key_path: str = ""
    # yearly: one request per year, the year goes into the endpoint path ("{year}"). Incremental
    # runs re-read the last refresh_years years up to the current one (sources without an
    # updated-at field, where past years still receive corrections).
    refresh_years: int = 2
    # scheduling and freshness: how often the dataset runs, and when it counts as late.
    # 0 = derived from frequency (warn after 1.5x, stale after 3x the interval).
    frequency: str = "daily"
    warn_after_hours: int = 0
    stale_after_hours: int = 0

    @property
    def frequency_hours(self) -> int:
        return FREQUENCY_HOURS[self.frequency]

    @property
    def warn_hours(self) -> float:
        return self.warn_after_hours or self.frequency_hours * WARN_FACTOR

    @property
    def stale_hours(self) -> float:
        return self.stale_after_hours or self.frequency_hours * STALE_FACTOR

    @property
    def dataset(self) -> str:
        return f"{self.source_system}.{self.entity}"

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "IngestionConfig":
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in row.items() if k in known and v is not None}
        if isinstance(data.get("request_params"), str):
            data["request_params"] = json.loads(data["request_params"] or "{}")
        for name in ("window_days", "overlap_days", "max_pages", "lookback_max_days", "refresh_years",
                     "warn_after_hours", "stale_after_hours"):
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
            if self.request_strategy not in INCREMENTAL_STRATEGIES:
                errors.append(f"incremental loads require request_strategy in {sorted(INCREMENTAL_STRATEGIES)}")
            if not self.watermark_field:
                errors.append("incremental loads require watermark_field")
            if not self.backfill_start:
                errors.append("incremental loads require backfill_start")
        if self.request_strategy == "yearly":
            if "{year}" not in self.endpoint:
                errors.append("yearly endpoint must contain '{year}'")
            if self.load_type != "incremental":
                errors.append("yearly requires load_type=incremental")
            if self.refresh_years < 1:
                errors.append("refresh_years must be >= 1")
        if self.request_strategy in DATE_PARAM_STRATEGIES:
            if not (self.date_param_start and self.date_param_end):
                errors.append(f"{self.request_strategy} requires date_param_start and date_param_end")
            if self.window_days < 1:
                errors.append("window_days must be >= 1")
        if self.request_strategy == "lookback":
            if self.load_type != "incremental":
                errors.append("lookback requires load_type=incremental")
            if not self.lookback_param:
                errors.append("lookback requires lookback_param")
            if self.lookback_max_days < 1:
                errors.append("lookback_max_days must be >= 1")
        if self.request_strategy == "fanout":
            if self.source_type != "REST" or self.load_type != "full":
                errors.append("fanout requires source_type=REST and load_type=full")
            if not (self.fanout_dataset and self.fanout_key_path):
                errors.append("fanout requires fanout_dataset and fanout_key_path")
            if "{key}" not in self.endpoint:
                errors.append("fanout endpoint must contain '{key}'")
            if self.pagination_type != "none":
                errors.append("fanout does not support pagination")
        if self.fanout_dataset and self.request_strategy != "fanout":
            errors.append("fanout_dataset is only valid with request_strategy=fanout")
        if self.fanout_dataset and not self.fanout_dataset.startswith(f"{self.source_system}."):
            errors.append("fanout_dataset must belong to the same source_system (datasets run in order inside one source)")
        if self.frequency not in FREQUENCY_HOURS:
            errors.append(f"frequency must be one of {sorted(FREQUENCY_HOURS)}")
        if self.warn_after_hours < 0 or self.stale_after_hours < 0:
            errors.append("warn_after_hours and stale_after_hours must be >= 0")
        if self.warn_after_hours and self.stale_after_hours and self.stale_after_hours < self.warn_after_hours:
            errors.append("stale_after_hours must be >= warn_after_hours")
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
        self.reset_counters()

    def reset_counters(self) -> None:
        """Per-dataset counters. The throttle state is kept, so one client shared by all
        datasets of a source enforces that source's rate limit across them."""
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


class LookbackGapError(RuntimeError):
    pass


def lookback_days(cfg: IngestionConfig, watermark: str, today: dt.date) -> int:
    """Days to request from a 'updated in the last N days' API: time since the watermark
    plus overlap. Fails loudly instead of silently missing updates beyond the API limit."""
    days = max((today - parse_date(watermark)).days + cfg.overlap_days, 1)
    if days > cfg.lookback_max_days:
        raise LookbackGapError(
            f"{cfg.dataset}: gap of {days} days since watermark {watermark} exceeds the "
            f"{cfg.lookback_max_days}-day lookback of {cfg.lookback_param}; run mode=backfill "
            f"with backfill_start <= {watermark} to recover"
        )
    return days


def yearly_range(cfg: IngestionConfig, run: RunContext, watermark: Optional[str], today: dt.date) -> List[int]:
    """Years to request for the yearly strategy.
    Backfill: backfill_start..backfill_end (defaults from config / today).
    Incremental without watermark: backfill_start year..current year.
    Incremental with watermark: the last refresh_years years up to the current one, extended
    back to the watermark year if runs were missed (so a long pause never skips a year)."""
    first_year = parse_date(cfg.backfill_start).year
    if run.mode == "backfill":
        # explicit backfills may reach before the configured start (e.g. CEAPS back to 2008)
        start = parse_date(run.backfill_start or cfg.backfill_start).year
        end = parse_date(run.backfill_end).year if run.backfill_end else today.year
    elif not watermark:
        start, end = first_year, today.year
    else:
        start = max(min(today.year - cfg.refresh_years + 1, int(str(watermark)[:4])), first_year)
        end = today.year
    return list(range(start, min(end, today.year) + 1))


def get_path(record: Any, path: str) -> Any:
    node = record
    for key in [k for k in (path or "").split(".") if k]:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _plan_rest_requests(cfg: IngestionConfig, run: RunContext, watermark: Optional[str], today: dt.date):
    """Return (year, window_start, window_end, params) for each request to make."""
    base_params = dict(cfg.request_params)
    if cfg.request_strategy == "single":
        return [(today.year, None, None, base_params)]
    if cfg.request_strategy == "yearly":
        return [
            (year, dt.date(year, 1, 1), min(dt.date(year, 12, 31), today), dict(base_params))
            for year in yearly_range(cfg, run, watermark, today)
        ]
    if cfg.request_strategy == "lookback" and run.mode == "incremental" and watermark:
        days = lookback_days(cfg, watermark, today)
        params = dict(base_params)
        params[cfg.lookback_param] = days
        return [(today.year, today - dt.timedelta(days=days), today, params)]
    start, end = resolve_date_range(cfg, run.mode, watermark, today, run.backfill_start, run.backfill_end)
    plan = []
    for w_start, w_end in date_windows(start, end, cfg.window_days):
        params = dict(base_params)
        params[cfg.date_param_start] = w_start.isoformat()
        params[cfg.date_param_end] = w_end.isoformat()
        plan.append((w_start.year, w_start, w_end, params))
    return plan


UNCHANGED_NOTE = "unchanged: identical to the last landed file (SHA-256), nothing written"


def snapshot_unchanged(cfg: IngestionConfig, store: Any, content: bytes) -> bool:
    """Write-time dedup for full snapshots: skip landing when the content is byte-identical
    to the most recent landed file of the dataset."""
    previous = store.last_landed_sha(cfg.dataset)
    return previous is not None and previous == sha256_hex(content)


def _landed_record(cfg, run, full_path, result, params, records, w_start, w_end) -> Dict[str, Any]:
    return {
        "execution_id": run.execution_id,
        "dataset": cfg.dataset,
        "file_path": full_path,
        "source_url": result.url,
        "request_params": json.dumps(params, sort_keys=True),
        "http_status": result.status,
        "bytes": len(result.content),
        "sha256": sha256_hex(result.content),
        "records": records,
        "window_start": w_start,
        "window_end": w_end,
        "landed_at": utc_now(),
    }


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
        if cfg.request_strategy == "fanout":
            landed, rows = _run_fanout(cfg, run, store, writer, http, headers, today)
            store.log_landed_files(landed)
            status, note = (STATUS_SUCCESS, "") if landed else (STATUS_SKIPPED, UNCHANGED_NOTE)
            record = _execution_record(cfg, run, status, rows=rows, files=len(landed), http=http, error=note)
            store.log_execution(record)
            return record
        plan = _plan_rest_requests(cfg, run, watermark_before, today)
        landed, rows, max_seen = [], 0, None
        for year, w_start, w_end, params in plan:
            if cfg.request_strategy == "single" and cfg.pagination_type == "none":
                result = http.get(build_url(cfg.base_url, cfg.endpoint), params=params, headers=headers)
                rows = len(extract_records(result.json(), cfg.record_path))
                if snapshot_unchanged(cfg, store, result.content):
                    record = _execution_record(cfg, run, STATUS_SKIPPED, rows=rows, http=http, error=UNCHANGED_NOTE)
                    store.log_execution(record)
                    return record
                rel_path = landing_file_path(cfg.landing_path, year, today, f"{run.pipeline_run_id}.json")
                landed.append(_landed_record(cfg, run, writer.write_bytes(rel_path, result.content), result, params, rows, None, None))
                continue
            url = build_url(cfg.base_url, cfg.endpoint.replace("{year}", str(year)))
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
                landed.append(_landed_record(cfg, run, full_path, result, req_params, len(records), w_start, w_end))
        watermark_after = None
        if cfg.load_type == "incremental":
            watermark_after = greatest(watermark_before, max_seen)
            if watermark_after and watermark_after != watermark_before:
                watermark_type = "YEAR" if cfg.request_strategy == "yearly" else "DATE"
                store.commit_watermark(cfg.dataset, watermark_type, watermark_after, run.execution_id)
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


def fanout_keys(cfg: IngestionConfig, store: Any, reader: Any) -> List[str]:
    """Distinct keys from the latest successful Bronze JSON files of cfg.fanout_dataset."""
    paths = [p for p in store.latest_landed_files(cfg.fanout_dataset) if p.endswith(".json")]
    if not paths:
        raise RuntimeError(
            f"{cfg.dataset}: no successful run of {cfg.fanout_dataset} to read fan-out keys from"
        )
    keys = set()
    for path in paths:
        payload = json.loads(reader.read_bytes(path).decode("utf-8"))
        for record in extract_records(payload, cfg.fanout_record_path):
            value = get_path(record, cfg.fanout_key_path)
            if value not in (None, ""):
                keys.add(str(value))
    if not keys:
        raise RuntimeError(f"{cfg.dataset}: no keys at '{cfg.fanout_key_path}' in {cfg.fanout_dataset}")
    return sorted(keys, key=lambda k: (len(k), k))


def _run_fanout(cfg, run, store, writer, http, headers, today):
    """One request per key, landed as a single JSON Lines file (one envelope per key with
    the untouched payload). A 404 for a key is recorded, not fatal; other errors fail the run."""
    from urllib.parse import quote

    keys = fanout_keys(cfg, store, writer)
    lines, rows = [], 0
    for key in keys:
        url = build_url(cfg.base_url, cfg.endpoint.replace("{key}", quote(key, safe="")))
        try:
            result = http.get(url, params=dict(cfg.request_params) or None, headers=headers)
            payload, status = result.json(), result.status
        except HttpError as exc:
            if exc.status != 404:
                raise
            payload, status = None, 404
        rows += len(extract_records(payload, cfg.record_path)) if payload is not None else 0
        lines.append(json.dumps({"key": key, "source_url": url, "http_status": status, "payload": payload}, ensure_ascii=False))
    content = ("\n".join(lines) + "\n").encode("utf-8")
    if snapshot_unchanged(cfg, store, content):
        return [], rows
    rel_path = landing_file_path(cfg.landing_path, today.year, today, f"{run.pipeline_run_id}.jsonl")
    landed = [{
        "execution_id": run.execution_id,
        "dataset": cfg.dataset,
        "file_path": writer.write_bytes(rel_path, content),
        "source_url": build_url(cfg.base_url, cfg.endpoint),
        "request_params": json.dumps({"fanout_dataset": cfg.fanout_dataset, "keys": len(keys), **cfg.request_params}, sort_keys=True),
        "http_status": 200,
        "bytes": len(content),
        "sha256": sha256_hex(content),
        "records": rows,
        "window_start": None,
        "window_end": None,
        "landed_at": utc_now(),
    }]
    return landed, rows


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
# Source runner: one notebook per source runs its datasets in order
# ---------------------------------------------------------------------------


class SourceRunError(RuntimeError):
    """Raised after all datasets of a source ran, when at least one failed, so the pipeline
    activity fails (and its handler runs) without one dataset blocking the others."""


def is_due(cfg: IngestionConfig, last_success_at: Optional[dt.datetime], now: dt.datetime) -> bool:
    if last_success_at is None:
        return True
    return now - last_success_at >= dt.timedelta(hours=cfg.frequency_hours * DUE_TOLERANCE)


def order_source_datasets(configs: List[IngestionConfig]) -> List[IngestionConfig]:
    """Keep config order, but run fan-outs after the dataset they read keys from."""
    plain = [c for c in configs if not c.fanout_dataset]
    fanouts = [c for c in configs if c.fanout_dataset]
    return plain + fanouts


def select_source_datasets(configs: List[IngestionConfig], source_system: str, entities: str = "") -> List[IngestionConfig]:
    wanted = {e.strip() for e in entities.split(",") if e.strip()}
    return order_source_datasets([
        c for c in configs
        if c.enabled and c.source_system == source_system and (not wanted or c.entity in wanted)
    ])


def compute_dataset_status(cfg: IngestionConfig, history: List[Dict[str, Any]], now: dt.datetime) -> Dict[str, Any]:
    """One status row per dataset from its execution history (any order). Absolute deadlines
    (warn_at, stale_at) let a query judge freshness at read time, even if runs stop."""
    runs = sorted((h for h in history if h["status"] != STATUS_NOT_DUE), key=lambda h: h["end_timestamp"])
    successes = [h for h in runs if h["status"] in SUCCESSFUL_STATUSES]
    loaded = [h for h in runs if h["status"] == STATUS_SUCCESS]
    last = runs[-1] if runs else None
    last_success_at = successes[-1]["end_timestamp"] if successes else None

    failures = 0
    for h in reversed(runs):
        if h["status"] in SUCCESSFUL_STATUSES:
            break
        failures += 1

    rows_last = loaded[-1]["rows_read"] if loaded else None
    previous = sorted(h["rows_read"] for h in loaded[-11:-1])
    rows_median = previous[len(previous) // 2] if previous else None
    # Volume is only comparable for full snapshots. Incremental row counts depend on how much
    # time each run's window covers (and are legitimately 0 in recess), so their health is
    # judged by freshness (last_success_at / stale_at) instead.
    if cfg.load_type != "full" or rows_last is None or rows_median is None:
        volume = "n/a"
    elif (rows_last == 0 and rows_median > 0) or (rows_median >= 50 and rows_last < 0.1 * rows_median):
        volume = "LOW"
    else:
        volume = "OK"

    def after(hours):
        return last_success_at + dt.timedelta(hours=hours) if last_success_at else None

    return {
        "dataset": cfg.dataset,
        "source_system": cfg.source_system,
        "entity": cfg.entity,
        "enabled": cfg.enabled,
        "frequency": cfg.frequency,
        "last_status": last["status"] if last else None,
        "last_run_at": last["end_timestamp"] if last else None,
        "last_success_at": last_success_at,
        "consecutive_failures": failures,
        "last_error": (last.get("error_message") or "")[:1000] if last and last["status"] == STATUS_FAILED else None,
        "rows_last_load": rows_last,
        "rows_median_recent": rows_median,
        "volume_flag": volume,
        "next_due_at": after(cfg.frequency_hours * DUE_TOLERANCE),
        "warn_at": after(cfg.warn_hours),
        "stale_at": after(cfg.stale_hours),
        "refreshed_at": now,
    }


def refresh_dataset_status(configs: List[IngestionConfig], store: Any, now: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
    now = now or utc_now()
    history = store.execution_history([c.dataset for c in configs])
    rows = [compute_dataset_status(c, history.get(c.dataset, []), now) for c in configs]
    store.upsert_dataset_status(rows)
    return rows


def run_source(
    source_system: str,
    configs: List[IngestionConfig],
    run: RunContext,
    store: Any,
    writer: Any,
    http: Optional[HttpClient] = None,
    secret_provider: Optional[Callable[[str], str]] = None,
    force: bool = False,
    entities: str = "",
    now: Optional[dt.datetime] = None,
    today: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Run every enabled dataset of one source, in dependency order, with one shared HTTP
    client (the source's rate limit applies across its datasets). Each dataset is isolated:
    a failure is logged and the next dataset still runs. Datasets that are not due are logged
    as NOT_DUE (incremental mode only; backfills and force=True run everything)."""
    now = now or utc_now()
    selected = select_source_datasets(configs, source_system, entities)
    if not selected:
        raise ConfigError(f"no enabled datasets for source '{source_system}'" + (f" matching '{entities}'" if entities else ""))
    http = http or HttpClient(rate_limit_per_second=min(c.rate_limit_per_second for c in selected))
    last_success = store.last_success_at([c.dataset for c in selected])

    results = []
    for cfg in selected:
        child = RunContext(
            pipeline_name=run.pipeline_name, pipeline_run_id=run.pipeline_run_id, mode=run.mode,
            backfill_start=run.backfill_start, backfill_end=run.backfill_end,
        )
        if run.mode == "incremental" and not force and not is_due(cfg, last_success.get(cfg.dataset), now):
            record = _execution_record(
                cfg, child, STATUS_NOT_DUE,
                error=f"not due: {cfg.frequency}, last success {last_success[cfg.dataset]:%Y-%m-%d %H:%M} UTC",
            )
            store.log_execution(record)
            results.append(record)
            continue
        http.reset_counters()
        try:
            if cfg.source_type == "REST":
                record = run_rest_ingestion(cfg, child, store, writer, http, secret_provider, today)
            else:
                record = run_file_ingestion(cfg, child, store, writer, http, today)
        except Exception as exc:  # already logged by the runner; keep going with the next dataset
            record = {"dataset": cfg.dataset, "status": STATUS_FAILED, "error_message": f"{type(exc).__name__}: {exc}"[:1000]}
        results.append(record)

    try:
        refresh_dataset_status(selected, store, now)
    except Exception as exc:  # status is derived data; never fail ingestion because of it
        print(f"warning: could not refresh {TABLE_DATASET_STATUS}: {exc}")

    summary = {
        "source_system": source_system,
        "datasets": {r["dataset"]: r["status"] for r in results},
        "failed": [r["dataset"] for r in results if r["status"] == STATUS_FAILED],
    }
    if summary["failed"]:
        details = "; ".join(f"{r['dataset']}: {(r.get('error_message') or '')[:200]}" for r in results if r["status"] == STATUS_FAILED)
        raise SourceRunError(f"{len(summary['failed'])} of {len(results)} datasets failed in {source_system}: {details}")
    return summary


def enabled_sources(configs: List[IngestionConfig], source_system: str = "", entities: str = "") -> List[Dict[str, Any]]:
    """Sources with at least one enabled dataset (optionally filtered), for the pipeline ForEach."""
    systems = {s.strip() for s in source_system.split(",") if s.strip()}
    counts: Dict[str, int] = {}
    for cfg in configs:
        if (not systems or cfg.source_system in systems) and select_source_datasets([cfg], cfg.source_system, entities):
            counts[cfg.source_system] = counts.get(cfg.source_system, 0) + 1
    return [{"source_system": s, "datasets": n} for s, n in sorted(counts.items())]


def log_pipeline_failure(
    configs: List[IngestionConfig], store: Any, *, source_system: str = "", entities: str = "",
    pipeline_name: str = "manual", pipeline_run_id: str = "manual", mode: str = "",
    error_message: str = "", activity_name: str = "", now: Optional[dt.datetime] = None,
) -> List[str]:
    """On-failure handler body: one FAILED row per affected dataset that has no row for this
    pipeline run yet (no duplicates), then refresh their status rows."""
    logged = []
    for dataset in failure_datasets(configs, source_system, entities):
        if store.has_execution(pipeline_run_id, dataset):
            continue
        store.log_execution(failure_record(dataset, pipeline_name, pipeline_run_id, mode, error_message, activity_name))
        logged.append(dataset)
    affected = [c for c in configs if c.dataset in set(logged)]
    if affected:
        try:
            refresh_dataset_status(affected, store, now)
        except Exception as exc:
            print(f"warning: could not refresh {TABLE_DATASET_STATUS}: {exc}")
    return logged


def failure_datasets(configs: List[IngestionConfig], source_system: str = "", entities: str = "") -> List[str]:
    """Datasets a pipeline-level failure should be recorded against: the source's enabled
    datasets, or the pipeline itself when no source is known (e.g. metadata read failed)."""
    if not source_system:
        return [PIPELINE_DATASET]
    selected = select_source_datasets(configs, source_system, entities)
    return [c.dataset for c in selected] or [f"{source_system}.*"]


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
        "rate_limit_per_second DOUBLE, lookback_param STRING, lookback_max_days INT, "
        "fanout_dataset STRING, fanout_record_path STRING, fanout_key_path STRING, refresh_years INT, "
        "frequency STRING, warn_after_hours INT, stale_after_hours INT, updated_at TIMESTAMP"
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
    TABLE_DATASET_STATUS: (
        "dataset STRING, source_system STRING, entity STRING, enabled BOOLEAN, frequency STRING, "
        "last_status STRING, last_run_at TIMESTAMP, last_success_at TIMESTAMP, consecutive_failures INT, "
        "last_error STRING, rows_last_load BIGINT, rows_median_recent BIGINT, volume_flag STRING, "
        "next_due_at TIMESTAMP, warn_at TIMESTAMP, stale_at TIMESTAMP, refreshed_at TIMESTAMP"
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

    def read_bytes(self, full_path: str) -> bytes:
        """Read a file previously written (full abfss path, as stored in etl_landed_file)."""
        handle, tmp = tempfile.mkstemp()
        os.close(handle)
        try:
            self.fs.cp(full_path, f"file://{tmp}")
            with open(tmp, "rb") as fh:
                return fh.read()
        finally:
            os.remove(tmp)


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

        # new config fields become new columns on the existing table
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
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

    def latest_landed(self, dataset: str) -> List[Tuple[str, str]]:
        """(file_path, sha256) of the most recent SUCCESS run of a dataset that wrote files."""
        from pyspark.sql import functions as F

        latest = (
            self.spark.read.format("delta").load(self.path(TABLE_EXECUTION_LOG))
            .where((F.col("dataset") == dataset) & (F.col("status") == STATUS_SUCCESS) & (F.col("files_written") > 0))
            .orderBy(F.col("end_timestamp").desc()).select("execution_id").limit(1).collect()
        )
        if not latest:
            return []
        return [
            (r["file_path"], r["sha256"]) for r in
            self.spark.read.format("delta").load(self.path(TABLE_LANDED_FILE))
            .where(F.col("execution_id") == latest[0]["execution_id"]).select("file_path", "sha256").collect()
        ]

    def latest_landed_files(self, dataset: str) -> List[str]:
        return [path for path, _ in self.latest_landed(dataset)]

    def last_landed_sha(self, dataset: str) -> Optional[str]:
        files = self.latest_landed(dataset)
        return files[0][1] if len(files) == 1 else None

    def execution_history(self, datasets: List[str], days: int = 400) -> Dict[str, List[Dict[str, Any]]]:
        from pyspark.sql import functions as F

        rows = (
            self.spark.read.format("delta").load(self.path(TABLE_EXECUTION_LOG))
            .where(F.col("dataset").isin(datasets) & (F.col("end_timestamp") >= F.expr(f"current_timestamp() - INTERVAL {days} DAYS")))
            .select("dataset", "status", "end_timestamp", "rows_read", "error_message").collect()
        )
        history: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            history.setdefault(row["dataset"], []).append(row.asDict())
        return history

    def last_success_at(self, datasets: List[str]) -> Dict[str, dt.datetime]:
        from pyspark.sql import functions as F

        rows = (
            self.spark.read.format("delta").load(self.path(TABLE_EXECUTION_LOG))
            .where(F.col("dataset").isin(datasets) & F.col("status").isin(list(SUCCESSFUL_STATUSES)))
            .groupBy("dataset").agg(F.max("end_timestamp").alias("last_success")).collect()
        )
        return {row["dataset"]: row["last_success"] for row in rows}

    def upsert_dataset_status(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        from delta.tables import DeltaTable

        src = self.spark.createDataFrame(rows, CONTROL_TABLE_SCHEMAS[TABLE_DATASET_STATUS])
        self._with_retry(lambda: (
            DeltaTable.forPath(self.spark, self.path(TABLE_DATASET_STATUS)).alias("t")
            .merge(src.alias("s"), "t.dataset = s.dataset")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        ))

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


def parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes")


def run_source_notebook(
    source_system: str, spark: Any, *, pipeline_name: str = "manual", pipeline_run_id: str = "manual",
    mode: str = "incremental", backfill_start: str = "", backfill_end: str = "",
    force: Any = False, entities: str = "", key_vault_url: str = "",
) -> Dict[str, Any]:
    """Fabric glue shared by the nb_bronze_<source> notebooks."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {sorted(MODES)}, got '{mode}'")
    workspace = current_workspace_name()
    store = ControlStore(spark, workspace)
    configs = [IngestionConfig.from_dict(r) for r in store.read_config()]
    run = RunContext(
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id if pipeline_run_id != "manual" else f"manual-{uuid.uuid4()}",
        mode=mode, backfill_start=backfill_start, backfill_end=backfill_end,
    )
    result = run_source(
        source_system, configs, run, store, OneLakeWriter(workspace),
        secret_provider=key_vault_secret_provider(key_vault_url) if key_vault_url else None,
        force=parse_bool(force), entities=entities,
    )
    print(json.dumps(result, indent=2, default=str))
    return result


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
