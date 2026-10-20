# Data Sources

Source discovery for Senado 360 (backlog E1.S4). Everything here was verified with live requests on **2026-10-19**. Re-check before relying on volumes or limits.

## Summary

| `source_system` | Source | Base URL | Auth | Format | Rate limit | Feeds |
|---|---|---|---|---|---|---|
| `senado_legislativo` | Senado — Dados Abertos Legislativos (v4.1) | `https://legis.senado.leg.br/dadosabertos` | None | JSON / XML / CSV | 10 req/s (HTTP 429 above), 503 under load | senators, mandates, parties, committees, matters, votes |
| `senado_admin` | Senado — Dados Abertos Administrativos (v2.3) | `https://adm.senado.gov.br/adm-dadosabertos/api/v1` | None | JSON (default) / CSV (`/csv` suffix) | Not documented. Assume the same as legislativo | CEAPS, contracts, procurement, suppliers |
| `transparencia` | Portal da Transparência — API | `https://api.portaldatransparencia.gov.br/api-de-dados` | API key (header `chave-api-dados`) | JSON | Per portal terms (see §3.2) | amendments (current) |
| `transparencia` | Portal da Transparência — bulk download | `https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO` | None | ZIP of `;`-separated CSV, Latin-1 | n/a | amendments (all years) |
| `ibge` | IBGE — Localidades + SIDRA | `https://servicodados.ibge.gov.br/api`, `https://apisidra.ibge.gov.br` | None | JSON | Not documented | municipalities, states, population |
| — | Base dos Dados | BigQuery `basedosdados.*` | GCP project | SQL | BigQuery quotas | reconciliation only (PRD §6.1, §42) |

OpenAPI specs (live):

- Legislativo: `https://legis.senado.leg.br/dadosabertos/v3/api-docs`
- Administrativo: `https://adm.senado.gov.br/adm-dadosabertos/v3/api-docs`
- Transparência: `https://api.portaldatransparencia.gov.br/v3/api-docs`

---

## 1. Senado Federal — Legislative (`senado_legislativo`)

**Conventions**

- JSON via the `Accept: application/json` header or a `.json` suffix.
- Legacy endpoints return deeply nested XML-style JSON (`{"ListaParlamentarEmExercicio": {"Metadados": …, "Parlamentares": {"Parlamentar": [...]}}}`). New endpoints (`/votacao`, `/processo`) return flat JSON arrays.
- Several legacy endpoints answer with **HTTP 301** to a static file (e.g. `/senador/partidos` → `/dados/ListaPartidos.json`). The HTTP client must follow redirects.
- **Deprecation:** many legacy endpoints are deprecated. Deprecated responses carry `Deprecation`, `Sunset`, and `Link: rel="successor"` headers. Ingestion should log these headers and alert on them. Use only the non-deprecated endpoints below.
- Rate limit: >10 req/s → HTTP 429. Avoid scheduling on round times (00:00, 01:15…). Retry 429/503 with backoff.
- No pagination: endpoints return the full result for the given filters.

### Endpoints

| Entity | Endpoint | Load type | Notes |
|---|---|---|---|
| Senators (current) | `GET /senador/lista/atual` | full | ~130 KB |
| Senators (history) | `GET /senador/lista/legislatura/{ini}/{fim}` | full | e.g. `56/57`. One row per senator per legislature (dedupe in Silver) |
| Senator detail | `GET /senador/{codigo}` | full (per senator) | |
| Mandates | `GET /senador/{codigo}/mandatos` | full (per senator) | Fan-out: one call per senator (~100–500 calls) |
| Party affiliations | `GET /senador/{codigo}/filiacoes` | full (per senator) | Needed for point-in-time party in `fact_vote` |
| Senators on leave | `GET /senador/afastados` | full | 301 → static file |
| Parties | `GET /senador/partidos` | full | 301 → `/dados/ListaPartidos.json`. Includes extinct parties |
| Legislatures | `GET /plenario/lista/legislaturas` | full | Reference for legislature/session periods |
| Committees | `GET /comissao/lista/colegiados` | full | Active only. Use `/comissao/lista/{tipo}` for others |
| Committee composition | `GET /composicao/comissao/{codigo}` | full (per committee) | `ativas=S` for current seats only |
| **Votes + individual votes** | `GET /votacao?dataInicio=YYYY-MM-DD&dataFim=YYYY-MM-DD` | **incremental** (date) | **Max 1-year window** (HTTP 400 otherwise). Each voting event embeds `votos[]` (one per senator). Secret votes carry totals only |
| Legislative matters | `GET /processo?…` | incremental | Requires a filter. `numdias=N` (≤30) returns processes **updated** in the last N days. Date filters are limited to 1-year windows |
| Rapporteurships | `GET /processo/relatoria?dataInicio=&dataFim=` | incremental | |
| Party orientation | `GET /plenario/votacao/orientacaoBancada/{ini}/{fim}` | incremental | Optional. Useful for party alignment |

### `/votacao` payload (key fields)

Voting event: `codigoSessaoVotacao`, `codigoSessao`, `dataSessao`, `idProcesso`, `codigoMateria`, `sigla`, `numero`, `ano`, `identificacao` (e.g. `PEC 221/2019`), `descricaoVotacao`, `resultadoVotacao`, `votacaoSecreta`, `totalVotosSim/Nao/Abstencao`, `votos[]`.

Individual vote (`votos[]`): `codigoParlamentar`, `nomeParlamentar`, `siglaPartidoParlamentar`, `siglaUFParlamentar`, `siglaVotoParlamentar` (e.g. `Sim`, `Não`, `Abstenção`, …), `descricaoVotoParlamentar`.

The party arrives on each vote, so `fact_vote.party_key` can come straight from the vote rather than from affiliation history.

### Volumes and history

| Year | Voting events | Individual votes |
|---|---|---|
| 2019 | 133 | 10,634 |
| 2025 (1-year call) | — | ~2.4 MB response |

History is available back to at least 1995, but yearly counts are uneven (e.g. 2015 returns only 17 events). **Recommendation:** backfill from **2019-02-01** (start of the 56th legislature). That gives two full legislatures and covers the CEAPS 2022+ window.

### Incremental strategy

- **Votes:** watermark = last `dataSessao` loaded. Each run requests `dataInicio = watermark - 7 days` (late corrections) up to today, then MERGEs on `codigoSessaoVotacao` + `codigoParlamentar`.
- **Matters:** daily `numdias=2` (overlap), MERGE on `idProcesso`. Backfill by year with `ano=`.
- **Reference entities:** full snapshot each run.

---

## 2. Senado Federal — Administrative (`senado_admin`)

**Conventions:** flat JSON arrays. Append `/csv` for CSV. No pagination: each call returns the full result.

### Endpoints

| Entity | Endpoint | Load type | Notes |
|---|---|---|---|
| **CEAPS** | `GET /senadores/despesas_ceaps/{ano}` | year snapshot | Whole year in one call (~6–7 MB, ~6 s) |
| Senators (admin view) | `GET /senadores` | full | |
| Resources per senator | `GET /senadores/{codigoParlamentar}/recursos-utilizados?ano=` | per senator | Optional |
| Contracts | `GET /contratacoes/contratos` | full | 2,516 contracts. Filter `anoEquals` works |
| Contract items | `GET /contratacoes/contratos/{id}/itens` | per contract | Fan-out |
| Contract payments | `GET /contratacoes/contratos/{id}/pagamentos` | per contract | Fan-out. Often empty |
| Contract amendments | `GET /contratacoes/contratos/{id}/aditivos` | per contract | |
| Procurement | `GET /contratacoes/licitacoes` | full | 2,766 processes. **The `aberturaRange*` filter returns 404**, so load the full list |
| Suppliers | `GET /contratacoes/empresas?pagina=` | full | **Paginated** (`pagina`). Embeds contracts |
| Commitment notes | `GET /contratacoes/notas_empenho` | full | Optional |

### CEAPS payload

`id`, `ano`, `mes`, `codSenador`, `nomeSenador`, `tipoDespesa`, `tipoDocumento`, `cpfCnpj`, `fornecedor`, `documento`, `data`, `detalhamento`, `valorReembolsado`.

- `codSenador` = the legislative API's `codigoParlamentar` (same code space). Senator matching is a direct join.
- `id` is the business key.
- `ano`/`mes` = competence period. `data` = document date, which can fall outside the competence year (2022 file contains dates from 2000-12-08 to 2023-12-23).

### Volumes and history

| Year | Rows | Size |
|---|---|---|
| 2008 | 2,449 | 0.85 MB |
| 2022 | 16,804 | 7.5 MB |
| 2026 (to date) | 15,637 | 6.6 MB |

- Data starts in **2008** (earlier years → 404).
- **Pre-2009 rows have no supplier, document, or date** (only aggregate values). Use 2022+ per the PRD, or 2019+ to align with votes.
- **Negative `valorReembolsado` exists** (30 rows in 2026), probably refunds/reversals. Needs an explicit rule (open decision #10).

### Incremental strategy

There is no updated-at field. Re-pull the **current and previous year** each run (late corrections) and MERGE on `id`. Detect deletions by comparing the year snapshot. Backfill: one call per year.

---

## 3. Portal da Transparência (`transparencia`)

### 3.1 Bulk download (primary)

| Property | Value |
|---|---|
| URL | `…/download-de-dados/emendas-parlamentares/UNICO` (302 → `dadosabertos-download.cgu.gov.br/…/EmendasParlamentares.zip`) |
| Size | ~32 MB zipped, ~256 MB unzipped |
| Coverage | **All years in one file, 2014–2026** (not per year) |
| Refresh | About monthly (`Last-Modified: 2026-10-10`) |
| Format | `;` separator, **Latin-1** encoding, decimal comma (`1300000,00`) |

Files in the ZIP:

| File | Rows | Grain | Key columns |
|---|---|---|---|
| `EmendasParlamentares.csv` | 94,663 | amendment × location × programmatic classification | Código da Emenda, Ano, Tipo, Código/Nome do Autor, Número, Localidade, **Código Município IBGE**, Município, Código UF IBGE, UF, Região, Função, Subfunção, Programa, Ação, Plano Orçamentário, Valor Empenhado, Valor Liquidado, Valor Pago, Restos a Pagar (Inscritos / Cancelados / Pagos) |
| `EmendasParlamentares_Convenios.csv` | 85,343 | amendment × agreement | Código da Emenda, Localidade, Data Publicação, Convenente, Objeto, Número/Valor Convênio |
| `EmendasParlamentares_PorFavorecido.csv` | 827,457 | amendment × recipient × month | Código da Emenda, Autor, Ano/Mês (2014-06 → 2026-10), Favorecido, Natureza Jurídica, Tipo, **UF/Município Favorecido** (names, no IBGE code), Valor Recebido |

Amendment types: Individual – Finalidade Definida (79,885), Individual – Transferências Especiais (5,108), Relator (3,892), Bancada (3,588), Comissão (2,190).

### 3.2 REST API (supplementary)

| Endpoint | Params | Notes |
|---|---|---|
| `GET /emendas` | `pagina`*, `ano`, `codigoEmenda`, `numeroEmenda`, `nomeAutor`, `tipoEmenda`, `codigoFuncao`, `codigoSubfuncao` | Paginated (15 per page) |
| `GET /emendas/documentos/{codigo}` | `pagina`* | Expenditure documents per amendment (fase, data, especieTipo) |

- **Auth:** free API key, registered at `https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email`, sent as header `chave-api-dados`. Without it the API returns 401.
- **Rate limit:** per the portal's API terms (commonly 90 req/min during the day, higher overnight). Confirm on registration and set pipeline concurrency accordingly.
- Response fields: `codigoEmenda`, `ano`, `tipoEmenda`, `autor`, `nomeAutor`, `numeroEmenda`, `localidadeDoGasto` (free text), `funcao`, `subfuncao`, `valorEmpenhado`, `valorLiquidado`, `valorPago`, `valorResto*`. **All values are strings.**
- The API has **no IBGE code and no program/action**, so it is poorer than the bulk file.

### Findings that change the PRD

1. **Bulk full snapshot beats bulk-history + API-incremental.** The whole history is one 32 MB file refreshed monthly. **Recommendation:** load the full bulk file each refresh (hash-compare to skip unchanged files), MERGE on the business key, and use the API only for optional intra-month freshness and `/documentos`. This simplifies E5.S1/E5.S2 and ADR-006.
2. **No "authorized amount".** Neither source exposes *valor autorizado/dotação*. Available stages: committed (empenhado), liquidated (liquidado), paid (pago), plus restos a pagar. PRD metrics *Authorized Amount* and *Payment Rate (paid/authorized)* need redefining, e.g. Payment Rate = paid / committed. Another option is a separate budget source (SIOP / Câmara dos Deputados' "Painel de Emendas").
3. **Only 40% of amendments have a municipality.** 37,702 rows carry a 7-digit IBGE code. 56,961 are `Sem informação` with location `MÚLTIPLO` (28k), `Nacional` (5k) or state-level `XX (UF)`. Municipality maps must either cover only the 40%, or use `PorFavorecido` (recipient municipality, matched by name + UF to IBGE). Document the choice in the `fact_amendment` grain (open decision #1).
4. **Authors include deputies and collective authors.** 1,493 distinct individual authors. Exact normalized-name matching against Senate senators (legislatures 55–57) finds 202. Senator attribution needs name normalization plus a curated mapping (Transparência `Código do Autor` → Senate `codigoParlamentar`). Some senators also authored amendments earlier as deputies; decide whether those count. Bancada/Comissão/Relator amendments have no individual senator.

---

## 4. IBGE (`ibge`)

| Entity | Endpoint | Load | Notes |
|---|---|---|---|
| Municipalities | `GET https://servicodados.ibge.gov.br/api/v1/localidades/municipios` | full | 5,571 municipalities with micro/meso/immediate regions, UF, region |
| States | `GET …/api/v1/localidades/estados` | full | 27 |
| Population estimate | `GET https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/9324/p/last` | yearly | Table 6579, var 9324. Latest = 2026. **No estimates for 2022–2023** (census years) |
| Census 2022 population | `GET https://apisidra.ibge.gov.br/values/t/4709/n6/all/v/93/p/2022` | once | 5,570 municipalities |

- The municipality count differs between sources (5,571 vs 5,570; a municipality was created after the census). Use the localidades list as the master.
- SIDRA's first row is a header-description row. Skip it.
- For per-capita metrics, use the estimate for the matching year, falling back to Census 2022 for 2022–2023.

---

## 5. Base dos Dados (reconciliation only)

Used only to reconcile aggregates (PRD §42): counts of senators/votes/expenses/amendments, sum of expenses, sum of paid amounts. Access needs a GCP project with BigQuery (free tier). Not a pipeline dependency. Datasets are identified during E7.S4.

---

## 6. Cross-source keys

| Join | Key | Quality |
|---|---|---|
| Legislative ↔ CEAPS | `codigoParlamentar` = `codSenador` | Exact (same code space) |
| Legislative ↔ Transparência amendments | Author name → senator | Fuzzy. Needs a mapping table (§3, finding 4) |
| Amendments ↔ IBGE | `Código Município IBGE` | Exact where present (40% of rows) |
| Amendment recipients ↔ IBGE | Município + UF name | Fuzzy (normalize accents/case, scope by UF) |
| Votes ↔ matters | `idProcesso` / `codigoMateria` | Exact |

## 7. Ingestion implications

| Concern | Rule |
|---|---|
| Redirects | Follow 301/302 (legislativo static files, Transparência bulk) |
| Retries | Exponential backoff on 429 and 503. Honor `Retry-After` |
| Throttling | ≤5 req/s to Senado APIs. Transparência limit set from its terms |
| Fan-out endpoints | `senador/{codigo}/*`, `contratos/{id}/*`, `composicao/comissao/{codigo}` → ForEach with bounded concurrency |
| Encoding | Transparência CSV = Latin-1, `;`, decimal comma. Senado = UTF-8 JSON |
| Deprecation | Log `Deprecation`/`Sunset` headers into `etl_execution_log` and alert |
| Secrets | Only the Transparência API key. Store it in Key Vault (E13.S2) |
