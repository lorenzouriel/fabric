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
# Reads `etl_ingestion_config` and returns the rows as a JSON array (notebook exit value).
# `pl_master_ingestion` filters them on `enabled` and fans out with ForEach.
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

def _wanted(value: str):
    return {v.strip() for v in value.split(",") if v.strip()}


systems, entities = _wanted(source_system), _wanted(entity)
rows = ControlStore(spark, current_workspace_name()).read_config()
rows = [
    IngestionConfig.from_dict(r).to_row()
    for r in rows
    if (not systems or r["source_system"] in systems) and (not entities or r["entity"] in entities)
]
print(f"{len(rows)} config rows ({sum(r['enabled'] for r in rows)} enabled)")
notebookutils.notebook.exit(json.dumps(rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
