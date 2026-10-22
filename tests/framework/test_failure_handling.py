import json

from conftest import REPO_ROOT

LOG_FAILURE_NOTEBOOK = "f3f468e1-03e5-4840-a694-8265ea20fb19"


# --- failure record ------------------------------------------------------------


def test_resolve_failure_dataset(common):
    cfg = json.dumps({"source_system": "senado_legislativo", "entity": "votes", "enabled": True})
    assert common.resolve_failure_dataset(cfg) == "senado_legislativo.votes"
    assert common.resolve_failure_dataset("{not json", "fallback.ds") == "fallback.ds"
    assert common.resolve_failure_dataset('{"entity": "votes"}') == common.PIPELINE_DATASET
    assert common.resolve_failure_dataset("", "") == "__pipeline__"


def test_failure_record_matches_execution_log_schema(common):
    record = common.failure_record("senado_legislativo.votes", "pl_master_ingestion", "run-1", "incremental",
                                   "MagicUsageError: Cannot read notebook", "REST Ingestion")
    ddl = common.CONTROL_TABLE_SCHEMAS[common.TABLE_EXECUTION_LOG]
    columns = [part.strip().split(" ")[0] for part in ddl.split(",")]
    assert list(record) == columns
    assert record["status"] == "FAILED"
    assert record["source_system"] == "senado_legislativo" and record["entity"] == "votes"
    assert record["error_message"].startswith("[pipeline activity 'REST Ingestion'] MagicUsageError")


def test_pipeline_level_failure_record(common):
    record = common.failure_record("__pipeline__", "pl_master_ingestion", "run-1", "incremental", "", "Read Ingestion Metadata")
    assert record["source_system"] is None and record["entity"] is None
    assert "no error message" in record["error_message"]


def test_failure_record_truncates_long_errors(common):
    record = common.failure_record("a.b", "p", "r", "incremental", "x" * 10000)
    assert len(record["error_message"]) == 4000


def test_runner_execution_record_has_same_columns(common):
    cfg = common.IngestionConfig.from_dict({
        "source_system": "s", "entity": "e", "source_type": "REST", "base_url": "https://x",
        "endpoint": "/e", "load_type": "full", "landing_path": "s/e",
    })
    runner_cols = list(common._execution_record(cfg, common.RunContext(), "SUCCESS"))
    handler_cols = list(common.failure_record("s.e", "p", "r", "incremental", "err"))
    assert runner_cols == handler_cols


# --- pipeline wiring -----------------------------------------------------------


def test_notebook_activities_share_a_spark_session_and_retry():
    """Trial capacities reject extra Spark sessions (HTTP 430) instead of queueing them:
    all notebook activities must share one high-concurrency session and retry transient errors."""
    for name, doc in _pipelines():
        for container in _containers(doc["properties"]["activities"]):
            for act in container:
                if act["type"] != "TridentNotebook":
                    continue
                assert act["typeProperties"].get("sessionTag") == "senado360_ingestion", f"{name}: '{act['name']}' needs sessionTag"
                assert act["policy"]["retry"] >= 2 and act["policy"]["retryIntervalInSeconds"] >= 60, f"{name}: '{act['name']}' needs retries"


def _containers(activities):
    """Yield every activity list in a pipeline (top level, ForEach, Switch, If)."""
    yield activities
    for act in activities:
        tp = act.get("typeProperties", {})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if key in tp:
                yield from _containers(tp[key])
        for case in tp.get("cases", []):
            yield from _containers(case["activities"])


def _pipelines():
    for content in REPO_ROOT.glob("pipelines/*.DataPipeline/pipeline-content.json"):
        yield content.parent.name, json.loads(content.read_text(encoding="utf-8"))


def test_every_notebook_activity_has_log_and_rethrow_on_failure():
    for name, doc in _pipelines():
        for container in _containers(doc["properties"]["activities"]):
            by_name = {a["name"]: a for a in container}
            for act in container:
                if act["type"] != "TridentNotebook" or act["typeProperties"]["notebookId"] == LOG_FAILURE_NOTEBOOK:
                    continue
                handlers = [
                    a for a in container
                    if a["type"] == "TridentNotebook"
                    and a["typeProperties"]["notebookId"] == LOG_FAILURE_NOTEBOOK
                    and {"activity": act["name"], "dependencyConditions": ["Failed"]} in a["dependsOn"]
                ]
                assert len(handlers) == 1, f"{name}: '{act['name']}' needs exactly one Failed -> nb_framework_log_failure handler"
                handler = handlers[0]
                assert f"activity('{act['name']}').error.message" in json.dumps(handler["typeProperties"]["parameters"])
                rethrows = [
                    a for a in container
                    if a["type"] == "Fail"
                    and {"activity": handler["name"], "dependencyConditions": ["Completed"]} in a["dependsOn"]
                ]
                assert len(rethrows) == 1, f"{name}: '{handler['name']}' must be followed by a Fail activity on Completed"
                assert set(by_name) >= {handler["name"], rethrows[0]["name"]}
