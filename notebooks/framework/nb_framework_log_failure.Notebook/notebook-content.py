# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_framework_log_failure
#
# Called by pipelines on the **Failed** path of a notebook activity, before a `Fail`
# activity re-raises the error. Covers failures the source notebook could not log itself:
# `%run` errors, Spark start-up, timeouts, cancels.
#
# Writes one FAILED row per enabled dataset of `source_system` that has no row for this
# `pipeline_run_id` yet (no duplicates), and refreshes their `etl_dataset_status` rows.
# Without `source_system` (e.g. the metadata read failed) it logs one `__pipeline__` row.

# PARAMETERS CELL ********************

source_system = ""
entities = ""
pipeline_name = "manual"
pipeline_run_id = "manual"
mode = ""
activity_name = ""
error_message = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_framework_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

store = ControlStore(spark, current_workspace_name())
try:
    configs = [IngestionConfig.from_dict(r) for r in store.read_config()]
except Exception as exc:  # the failure being logged may be a broken config
    print(f"warning: could not read config: {exc}")
    configs = []

logged = log_pipeline_failure(
    configs, store, source_system=source_system, entities=entities,
    pipeline_name=pipeline_name, pipeline_run_id=pipeline_run_id, mode=mode,
    error_message=error_message, activity_name=activity_name,
)
outcome = f"logged FAILED for {logged}" if logged else f"nothing to log: rows already exist for run {pipeline_run_id}"
print(outcome)
notebookutils.notebook.exit(outcome)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
