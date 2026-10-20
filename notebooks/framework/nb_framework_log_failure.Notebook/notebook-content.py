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
# activity re-raises the error. Covers failures that happen before (or outside) the
# runner's own error handling: `%run` errors, Spark start-up, bad parameters, timeouts.
#
# Writes one FAILED row to `etl_execution_log`, unless the failed notebook already logged
# a row for this `pipeline_run_id` + dataset (no duplicates).

# PARAMETERS CELL ********************

config_json = ""
dataset = ""
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

failed_dataset = resolve_failure_dataset(config_json, dataset)
store = ControlStore(spark, current_workspace_name())

if store.has_execution(pipeline_run_id, failed_dataset):
    outcome = f"skipped: {failed_dataset} already logged for run {pipeline_run_id}"
else:
    store.log_execution(failure_record(failed_dataset, pipeline_name, pipeline_run_id, mode, error_message, activity_name))
    outcome = f"logged FAILED for {failed_dataset} (run {pipeline_run_id})"

print(outcome)
notebookutils.notebook.exit(outcome)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
