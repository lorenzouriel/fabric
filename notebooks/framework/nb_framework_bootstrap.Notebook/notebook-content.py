# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_framework_bootstrap
#
# Idempotent. Run after deploying, and again whenever `nb_framework_config` changes.
#
# 1. Creates the control tables in `lh_senado_control` if they don't exist.
# 2. Validates every config row, then MERGEs them into `etl_ingestion_config`.

# CELL ********************

%run nb_framework_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_framework_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

configs = [IngestionConfig.from_dict(row) for row in INGESTION_CONFIG]

store = ControlStore(spark, current_workspace_name())
store.ensure_tables()
store.upsert_config(configs)

display(spark.read.format("delta").load(store.path(TABLE_CONFIG)).orderBy("source_system", "entity"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
