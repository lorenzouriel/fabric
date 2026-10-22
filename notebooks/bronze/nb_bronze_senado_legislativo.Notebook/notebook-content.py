# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_senado_legislativo
#
# Source: Senado Federal, Dados Abertos Legislativos (`legis.senado.leg.br`, 10 req/s limit).
# Runs every enabled `senado_legislativo` dataset from `etl_ingestion_config`, in order
# (fan-outs such as `party_affiliations` after `senators_history`), with one shared HTTP client.
#
# - Datasets that are not due (their `frequency`) are logged as NOT_DUE. `force = true` runs all.
# - A failed dataset is logged and the next one still runs; the notebook fails at the end.
# - `entities` limits the run to some datasets, e.g. `votes,matters` (manual runs, backfills).

# PARAMETERS CELL ********************

pipeline_name = "manual"
pipeline_run_id = "manual"
mode = "incremental"
backfill_start = ""
backfill_end = ""
force = "false"
entities = ""
key_vault_url = ""

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

result = run_source_notebook(
    "senado_legislativo", spark,
    pipeline_name=pipeline_name, pipeline_run_id=pipeline_run_id, mode=mode,
    backfill_start=backfill_start, backfill_end=backfill_end,
    force=force, entities=entities, key_vault_url=key_vault_url,
)
notebookutils.notebook.exit(json.dumps(result, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
