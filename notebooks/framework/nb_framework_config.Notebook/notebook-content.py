# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_framework_config
#
# Ingestion metadata, versioned in Git. `nb_framework_bootstrap` MERGEs these rows into
# `lh_senado_control.dbo.etl_ingestion_config`. Rows removed from this list are set to
# `enabled = false` (never deleted), so their history stays readable.
#
# Endpoint details and API limits: `docs/sources.md`.

# CELL ********************

SENADO_LEGIS_URL = "https://legis.senado.leg.br/dadosabertos"

INGESTION_CONFIG = [
    {
        "source_system": "senado_legislativo",
        "entity": "senators",
        "description": "Senators currently in office (full snapshot).",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/senador/lista/atual",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ListaParlamentarEmExercicio.Parlamentares.Parlamentar",
        "landing_path": "senado_legislativo/senators",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "senators_history",
        "description": "Senators of legislatures 56-57 (2019-2027), full snapshot. Extend the range when legislature 58 starts (Feb 2027).",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/senador/lista/legislatura/56/57",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ListaParlamentarLegislatura.Parlamentares.Parlamentar",
        "landing_path": "senado_legislativo/senators_history",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "votes",
        "description": "Plenary voting events with embedded individual votes (votos[]). API limit: 1-year window per call.",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/votacao",
        "load_type": "incremental",
        "request_strategy": "date_window",
        "date_param_start": "dataInicio",
        "date_param_end": "dataFim",
        "window_days": 366,  # windows are also split per calendar year; 366 covers leap years in one call
        "overlap_days": 7,
        "watermark_field": "dataSessao",
        "backfill_start": "2019-02-01",
        "record_path": "",
        "landing_path": "senado_legislativo/votes",
        "rate_limit_per_second": 5.0,
    },
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
