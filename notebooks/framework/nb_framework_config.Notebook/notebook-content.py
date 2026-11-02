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
SENADO_ADMIN_URL = "https://adm.senado.gov.br/adm-dadosabertos/api/v1"
TRANSPARENCIA_PORTAL_URL = "https://portaldatransparencia.gov.br"
TRANSPARENCIA_API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
# portal terms: 90 requests/min 06:00-23:59 (higher overnight); stay under the daytime limit
TRANSPARENCIA_RATE_LIMIT = 1.4
IBGE_LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1"
IBGE_SIDRA_URL = "https://apisidra.ibge.gov.br"
IBGE_RATE_LIMIT = 2.0

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
    {
        "source_system": "senado_legislativo",
        "entity": "parties",
        "description": "Political parties, active and extinct (full snapshot). Endpoint 301-redirects to a static file.",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/senador/partidos",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ListaPartidos.Partidos.Partido",
        "landing_path": "senado_legislativo/parties",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "party_affiliations",
        "description": "Party affiliation history per senator (join/leave dates). One call per senator in senators_history.",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/senador/{key}/filiacoes",
        "load_type": "full",
        "request_strategy": "fanout",
        "fanout_dataset": "senado_legislativo.senators_history",
        "fanout_record_path": "ListaParlamentarLegislatura.Parlamentares.Parlamentar",
        "fanout_key_path": "IdentificacaoParlamentar.CodigoParlamentar",
        "record_path": "FiliacaoParlamentar.Parlamentar.Filiacoes.Filiacao",
        "landing_path": "senado_legislativo/party_affiliations",
        "rate_limit_per_second": 5.0,
        "frequency": "weekly",  # 318 calls; affiliations change rarely
    },
    {
        "source_system": "senado_legislativo",
        "entity": "committees",
        "description": "Committees currently active in the Senate and Congress (full snapshot). Endpoint 301-redirects to a static file.",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/comissao/lista/colegiados",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ListaColegiados.Colegiados.Colegiado",
        "landing_path": "senado_legislativo/committees",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "committee_composition_permanent",
        "description": "Current membership of permanent committees (blocs, parties, members, roles).",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/composicao/lista/permanente",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ComissoesPermanentes.Colegiados.Colegiado",
        "landing_path": "senado_legislativo/committee_composition_permanent",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "committee_composition_cpi",
        "description": "Current membership of parliamentary inquiry committees (CPIs).",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/composicao/lista/cpi",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ComissoesInquerito.Colegiados.Colegiado",
        "landing_path": "senado_legislativo/committee_composition_cpi",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "committee_composition_temporary",
        "description": "Current membership of temporary committees.",
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/composicao/lista/temporaria",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "ComissoesTemporarias.Colegiados.Colegiado",
        "landing_path": "senado_legislativo/committee_composition_temporary",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "matters",
        "description": (
            "Legislative matters (processos). First run/backfill: presented since 2019-02-01 in 1-year windows. "
            "Incremental: numdias lookback (API max 30 days) catches status updates."
        ),
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/processo",
        "load_type": "incremental",
        "request_strategy": "lookback",
        "lookback_param": "numdias",
        "lookback_max_days": 30,
        "date_param_start": "dataInicioApresentacao",
        "date_param_end": "dataFimApresentacao",
        "window_days": 366,
        "overlap_days": 1,
        "watermark_field": "dataUltimaAtualizacao",
        "backfill_start": "2019-02-01",
        "record_path": "",
        "landing_path": "senado_legislativo/matters",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_legislativo",
        "entity": "rapporteurships",
        "description": (
            "Rapporteur designations by designation date (1-year windows). The 30-day overlap re-reads recent "
            "designations to pick up removals (dataDestituicao); older removals need a periodic backfill."
        ),
        "source_type": "REST",
        "base_url": SENADO_LEGIS_URL,
        "endpoint": "/processo/relatoria",
        "load_type": "incremental",
        "request_strategy": "date_window",
        "date_param_start": "dataInicio",
        "date_param_end": "dataFim",
        "window_days": 366,
        "overlap_days": 30,
        "watermark_field": "dataDesignacao",
        "backfill_start": "2019-02-01",
        "record_path": "",
        "landing_path": "senado_legislativo/rapporteurships",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_admin",
        "entity": "ceaps",
        "description": (
            "CEAPS parliamentary expenses, one call per year (year in the path). No updated-at field: "
            "incremental runs re-read the current and previous year to pick up late filings and corrections."
        ),
        "source_type": "REST",
        "base_url": SENADO_ADMIN_URL,
        "endpoint": "/senadores/despesas_ceaps/{year}",
        "load_type": "incremental",
        "request_strategy": "yearly",
        "refresh_years": 2,
        "watermark_field": "ano",
        "backfill_start": "2019-01-01",
        "record_path": "",
        "landing_path": "senado_admin/ceaps",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_admin",
        "entity": "contracts",
        "description": "All Senate contracts (full snapshot, ~2.5k). Embeds supplier, procurement reference and managing units.",
        "source_type": "REST",
        "base_url": SENADO_ADMIN_URL,
        "endpoint": "/contratacoes/contratos",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "senado_admin/contracts",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_admin",
        "entity": "contract_items",
        "description": (
            "Items per contract, one call per contract in `contracts`. Most contracts have no items (HTTP 404, "
            "recorded per key). Payments (/pagamentos) are not ingested: empty for every contract sampled."
        ),
        "source_type": "REST",
        "base_url": SENADO_ADMIN_URL,
        "endpoint": "/contratacoes/contratos/{key}/itens",
        "load_type": "full",
        "request_strategy": "fanout",
        "fanout_dataset": "senado_admin.contracts",
        "fanout_record_path": "",
        "fanout_key_path": "id",
        "record_path": "",
        "landing_path": "senado_admin/contract_items",
        "rate_limit_per_second": 5.0,
        "frequency": "weekly",  # 2,525 calls (~8.5 min); items change rarely
    },
    {
        "source_system": "senado_admin",
        "entity": "procurement",
        "description": "Procurement processes (licitações) with lots/details (full snapshot). The date filter is broken upstream (404).",
        "source_type": "REST",
        "base_url": SENADO_ADMIN_URL,
        "endpoint": "/contratacoes/licitacoes",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "senado_admin/procurement",
        "rate_limit_per_second": 5.0,
    },
    {
        "source_system": "senado_admin",
        "entity": "suppliers",
        "description": "Contracted companies/people with their contracts (full snapshot). Omitting `pagina` returns all rows.",
        "source_type": "REST",
        "base_url": SENADO_ADMIN_URL,
        "endpoint": "/contratacoes/empresas",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "senado_admin/suppliers",
        "rate_limit_per_second": 5.0,
    },
    # ---------------------------------------------------------------- Portal da Transparência
    {
        "source_system": "transparencia",
        "entity": "amendments",
        "description": (
            "Parliamentary amendments, all years (2014-) in one bulk ZIP refreshed about monthly: "
            "EmendasParlamentares, _Convenios and _PorFavorecido CSVs (Latin-1, ';'). Primary source. "
            "Downloaded weekly; skipped when the SHA-256 matches a file already landed."
        ),
        "source_type": "FILE",
        "base_url": TRANSPARENCIA_PORTAL_URL,
        "endpoint": "/download-de-dados/emendas-parlamentares/UNICO",
        "load_type": "full",
        "request_strategy": "single",
        "landing_path": "transparencia/amendments",
        "frequency": "weekly",
        "rate_limit_per_second": TRANSPARENCIA_RATE_LIMIT,
    },
    {
        "source_system": "transparencia",
        "entity": "amendments_api",
        "description": (
            "Amendments from the REST API (15 per page) for the current year only: intra-month freshness "
            "between bulk refreshes. Poorer than the bulk file (no IBGE code, no program/action). "
            "Silver takes the newer of bulk vs API per amendment (docs/sources.md, bulk/API rule)."
        ),
        "source_type": "REST",
        "base_url": TRANSPARENCIA_API_URL,
        "endpoint": "/emendas",
        "load_type": "incremental",
        "request_strategy": "yearly",
        "request_params": {"ano": "{year}"},
        "refresh_years": 1,
        "watermark_field": "ano",
        "backfill_start": "2026-01-01",
        "record_path": "",
        "pagination_type": "page",
        "page_param": "pagina",
        "authentication_type": "api_key_header",
        "auth_header": "chave-api-dados",
        "secret_name": "transparencia-api-key",
        "landing_path": "transparencia/amendments_api",
        "rate_limit_per_second": TRANSPARENCIA_RATE_LIMIT,
    },
    # ---------------------------------------------------------------- IBGE
    {
        "source_system": "ibge",
        "entity": "municipalities",
        "description": "All municipalities with micro/meso/immediate/intermediate regions, UF and region (~5.6k, full snapshot).",
        "source_type": "REST",
        "base_url": IBGE_LOCALIDADES_URL,
        "endpoint": "/localidades/municipios",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "ibge/municipalities",
        "frequency": "monthly",
        "rate_limit_per_second": IBGE_RATE_LIMIT,
    },
    {
        "source_system": "ibge",
        "entity": "states",
        "description": "The 27 federative units with region (full snapshot).",
        "source_type": "REST",
        "base_url": IBGE_LOCALIDADES_URL,
        "endpoint": "/localidades/estados",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "ibge/states",
        "frequency": "monthly",
        "rate_limit_per_second": IBGE_RATE_LIMIT,
    },
    {
        "source_system": "ibge",
        "entity": "population_estimates",
        "description": (
            "Estimated resident population per municipality (SIDRA table 6579, variable 9324), one call per "
            "year (SIDRA caps a request at 50k values). First record of each file is a header row. "
            "2022-2023 have no estimate (census years): those files hold only the header."
        ),
        "source_type": "REST",
        "base_url": IBGE_SIDRA_URL,
        "endpoint": "/values/t/6579/n6/all/v/9324/p/{year}",
        "load_type": "incremental",
        "request_strategy": "yearly",
        "refresh_years": 1,
        "watermark_field": "D3C",
        "backfill_start": "2014-01-01",
        "record_path": "",
        "landing_path": "ibge/population_estimates",
        "frequency": "monthly",
        "rate_limit_per_second": IBGE_RATE_LIMIT,
    },
    {
        "source_system": "ibge",
        "entity": "population_census_2022",
        "description": "Census 2022 resident population per municipality (SIDRA table 4709, variable 93). Fallback for 2022-2023 per-capita.",
        "source_type": "REST",
        "base_url": IBGE_SIDRA_URL,
        "endpoint": "/values/t/4709/n6/all/v/93/p/2022",
        "load_type": "full",
        "request_strategy": "single",
        "record_path": "",
        "landing_path": "ibge/population_census_2022",
        "frequency": "monthly",
        "rate_limit_per_second": IBGE_RATE_LIMIT,
    },
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
