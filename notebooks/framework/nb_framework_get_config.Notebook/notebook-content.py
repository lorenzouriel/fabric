# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_framework_get_config
#
# Reads `etl_ingestion_config` and returns the **sources** that have enabled datasets, as a
# JSON array (notebook exit value), e.g. `[{"source_system": "senado_admin", "datasets": 5}]`.
# `pl_master_ingestion` runs one source notebook per item.
# Optional filters: `source_system`, `entity` (comma-separated).

# PARAMETERS CELL ********************

source_system = ""
entity = ""

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

configs = [IngestionConfig.from_dict(r) for r in ControlStore(spark, current_workspace_name()).read_config()]
sources = enabled_sources(configs, source_system, entity)
print(json.dumps(sources, indent=2))
notebookutils.notebook.exit(json.dumps(sources))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
