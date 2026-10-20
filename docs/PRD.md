# Product Requirements Document

## 1. Product Overview

### Product Name

**Brazilian Senate Data & AI Platform**

### Working Portfolio Name

**Senado 360 — Data, Analytics & AI on Microsoft Fabric**

### Product Type

End-to-end Data Engineering, Analytics Engineering, Business Intelligence, and AI portfolio project built primarily on Microsoft Fabric.

### Context

The project is based on the Data Glow Up #48 challenge, which uses Brazilian public-government datasets covering:

- legislative activity of the Federal Senate;
- nominal voting records;
- senators and political parties;
- committees and legislative matters;
- parliamentary quota expenses;
- contracts and procurement;
- parliamentary amendments;
- municipalities receiving amendment funding.

Instead of consuming only curated datasets from Base dos Dados, the project will ingest primarily from the original government sources and APIs.

The platform will demonstrate how a modern data architecture can ingest, standardize, model, govern, analyze, and expose Brazilian public-sector data through both traditional BI and conversational AI.

---

# 2. Product Vision

Build a public-data platform that makes Senate activity easier to explore while demonstrating production-oriented Microsoft Fabric architecture.

The platform should answer questions such as:

- How has a senator voted?
- Which senators vote similarly?
- How much does each senator spend through CEAPS?
- How does a senator's spending compare with their own history and peers?
- Where are parliamentary amendments being allocated?
- Which municipalities receive the most resources?
- How much of the authorized amendment value has actually been paid?
- What legislative or financial patterns stand out in the data?
- What does the latest available data indicate about a senator, party, municipality, or legislative matter?

The project should provide two complementary interfaces:

1. **Power BI** for structured visual exploration.
2. **AI Agent** for natural-language analytical questions.

---

# 3. Product Goals

## 3.1 Primary Goals

The project must demonstrate competency across:

### Data Engineering

- REST API ingestion
- bulk-file ingestion
- parameterized pipelines
- metadata-driven ingestion
- incremental data loading
- historical backfills
- schema management
- retry and failure handling
- Delta Lake
- medallion architecture
- PySpark transformations
- data-quality validation
- observability
- audit logging

### Analytics Engineering

- dimensional modeling
- business metric definitions
- reusable analytical views
- semantic modeling
- analytical consistency between BI and AI

### Business Intelligence

- Power BI semantic model
- curated measures
- visual storytelling
- transparent methodology
- drill-through analysis

### Generative AI

- Fabric Data Agent
- governed natural-language access to analytical data
- LangGraph orchestration
- deterministic analytical tools
- data provenance
- methodology explanations
- AI guardrails

### Architecture

- separation of ingestion, transformation, serving, BI, and AI concerns;
- clear ownership of each architectural component;
- justified technology choices;
- documented architectural decisions;
- CI/CD-oriented project organization.

---

# 4. Non-Goals

The first release will not attempt to build:

- a real-time streaming architecture;
- Fabric Eventstream;
- Kafka infrastructure;
- Eventhouse-based streaming analytics;
- automatic political sentiment classification;
- prediction of election results;
- ideological classification of individual senators;
- automated accusations of corruption or wrongdoing;
- production-scale public social-media automation;
- a universal political scoring system;
- a complete replacement for official government portals.

The product focuses on transparent analysis of published public data.

---

# 5. Target Users

## 5.1 Primary User — Data Explorer

A citizen, journalist, analyst, student, or politically interested user who wants to understand Senate activity without manually navigating several government portals.

Typical questions:

- How much did Senator X spend?
- Where did Senator X allocate amendments?
- How did Senator X vote on a specific matter?
- Which senators vote most similarly?
- Which municipality received the most amendment funding?

---

## 5.2 Secondary User — Data Engineer / Technical Recruiter

Someone evaluating the technical quality of the project.

They should be able to understand:

- source architecture;
- ingestion patterns;
- medallion implementation;
- transformation framework;
- dimensional model;
- data-quality controls;
- monitoring;
- deployment model;
- AI architecture.

---

## 5.3 Secondary User — Analyst

Someone interested in performing exploratory analysis against curated datasets.

They should have access to:

- Gold Warehouse;
- analytical views;
- semantic model;
- documented metrics;
- reusable dimensions and facts.

---

# 6. Core Product Principles

## 6.1 Official Sources First

Primary ingestion must use original government sources wherever practical.

Base dos Dados may be used for:

- reconciliation;
- data validation;
- benchmarking;
- identifying discrepancies.

It must not be the primary dependency of the platform.

---

## 6.2 One Governed Business Layer

Power BI and AI should derive their answers from the same governed analytical model.

The platform must avoid situations where:

- Power BI defines one formula;
- the AI agent independently invents another.

Canonical business metrics should be implemented once and reused.

---

## 6.3 Raw Data Must Be Recoverable

Bronze must preserve the source representation sufficiently to support:

- replay;
- debugging;
- backfills;
- reprocessing;
- source reconciliation.

---

## 6.4 AI Must Not Manufacture Political Conclusions

The system may identify:

- anomalies;
- trends;
- ranking differences;
- correlations;
- similarities;
- historical patterns.

It must not automatically equate those patterns with:

- corruption;
- fraud;
- misconduct;
- ideological intent;
- coordination;
- criminality.

---

# 7. High-Level Architecture

```text
                     OFFICIAL GOVERNMENT SOURCES
                                │
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
          ▼                     ▼                      ▼
 Senate Legislative      Senate Administrative   Transparency Portal
 REST / Web APIs             REST API            API + Bulk Files
          │                     │                      │
          └─────────────────────┼──────────────────────┘
                                │
                                ▼
                      FABRIC DATA FACTORY
                                │
                    Metadata-Driven Pipelines
                                │
                                ▼
                      BRONZE LAKEHOUSE
                    Raw / Immutable / Audit
                                │
                                ▼
                      PySpark Transformations
                                │
                   Data Quality + Standardization
                                │
                                ▼
                      SILVER LAKEHOUSE
                   Clean / Conformed / Delta
                                │
                                ▼
                       Business Modeling
                                │
                                ▼
                       GOLD WAREHOUSE
                    Dimensional Model + Views
                                │
               ┌────────────────┴────────────────┐
               │                                 │
               ▼                                 ▼
        Semantic Model                    AI Serving Views
               │                                 │
               ▼                                 ▼
           Power BI                       Fabric Data Agent
                                                   │
                                                   │ MCP
                                                   ▼
                                               LangGraph
                                                   │
                                  ┌────────────────┼───────────────┐
                                  ▼                ▼               ▼
                             Data Queries      Metrics       Provenance
                                                   │
                                                   ▼
                                           Conversational UI
```

---

# 8. Data Sources

## 8.1 Senado Federal — Legislative

The legislative source should provide entities including:

- senators;
- mandates;
- parties;
- committee membership;
- legislative matters;
- sessions;
- nominal votes;
- individual senator votes;
- rapporteurs;
- authorship;
- legislative activity.

### Expected ingestion pattern

Small reference entities:

**Full snapshot**

Examples:

- senators;
- political parties;
- committees.

Transactional entities:

**Incremental ingestion**

Examples:

- votes;
- legislative matters;
- sessions.

---

# 9. Senado Federal — Administrative

Administrative data should include:

- CEAPS parliamentary expenses;
- suppliers;
- contracts;
- procurement processes;
- contract items;
- contract payments where available.

### Expected ingestion pattern

Historical load:

```text
year-by-year
```

Incremental:

```text
current year / recent periods
```

Example:

```text
2022
2023
2024
2025
2026
```

Each execution should track source period and ingestion date.

---

# 10. Portal da Transparência

Primary domain:

**Parliamentary Amendments**

Required concepts may include:

- amendment;
- parliamentarian;
- municipality;
- authorized amount;
- committed amount;
- liquidated amount;
- paid amount;
- expenditure documents;
- year;
- program/function where available.

### Ingestion strategy

Historical:

**bulk downloadable datasets**

Current/recent:

**REST API**

Reason:

Large historical datasets should not rely exclusively on API requests where official bulk data is more appropriate.

---

# 11. Ingestion Architecture

## 11.1 Metadata-Driven Framework

A configuration table should control ingestion.

Example:

| Field | Description |
|---|---|
| source_system | Logical source |
| entity | Dataset |
| source_type | REST / FILE |
| base_url | Source URL |
| endpoint | API resource |
| load_type | full / incremental |
| watermark_field | Incremental field |
| landing_path | Bronze destination |
| enabled | Active configuration |
| pagination_type | API pagination |
| authentication_type | None/token/etc. |

Example rows:

```text
senado_legislativo | senators   | REST | full
senado_legislativo | votes      | REST | incremental
senado_admin       | ceaps      | REST | incremental
transparencia      | amendments | FILE | historical
transparencia      | amendments | REST | incremental
```

---

# 12. Pipeline Design

Primary orchestration:

```text
pl_master_ingestion
```

Flow:

```text
Read ingestion metadata
        │
        ▼
Filter enabled datasets
        │
        ▼
ForEach source
        │
        ├── REST ingestion
        │
        └── File ingestion
        │
        ▼
Land Bronze
        │
        ▼
Write audit record
```

Downstream orchestration:

```text
pl_master_data_platform
       │
       ├── pl_ingestion
       │
       ├── nb_silver_run
       │
       ├── dq_validation
       │
       ├── nb_gold_run
       │
       └── semantic refresh
```

---

# 13. Bronze Layer

## Purpose

Preserve source data with minimal transformation.

Bronze must allow reprocessing without retrieving the entire dataset again when practical.

Example:

```text
lh_senado_bronze
```

Potential organization:

```text
Files/
    senate_legislative/
        senators/
        votes/
        matters/

    senate_administrative/
        ceaps/
        contracts/
        procurement/

    transparency/
        amendments/
```

Partition example:

```text
/source=senado
/entity=ceaps
/year=2026
/ingestion_date=2026-10-19
```

---

# 14. Technical Metadata

Bronze and/or downstream tables should track:

```text
_ingestion_timestamp
_ingestion_date
_source_system
_source_entity
_pipeline_run_id
_source_file
_source_url
_record_hash
```

Where applicable:

```text
_source_updated_at
```

---

# 15. Silver Layer

## Purpose

Create trusted, normalized, reusable entities.

Example:

```text
lh_senado_silver
```

Silver responsibilities include:

- schema validation;
- column standardization;
- null handling;
- deduplication;
- type conversion;
- date normalization;
- monetary normalization;
- political-party standardization;
- state normalization;
- municipality normalization;
- IBGE municipality matching;
- business-key creation;
- referential validation;
- duplicate API-result handling.

---

# 16. Silver Entities

Expected tables include:

```text
senator
senator_mandate
party
committee
committee_member

legislative_matter
legislative_session
vote
senator_vote

expense
expense_category
supplier

contract
procurement

amendment
amendment_payment

municipality
state
```

Silver remains primarily entity-oriented rather than report-oriented.

---

# 17. Delta Lake Requirements

Silver should primarily use Delta tables.

The implementation should demonstrate where relevant:

- MERGE;
- upsert;
- schema evolution;
- incremental transformation;
- partition management;
- idempotent processing;
- table optimization.

Every incremental process should be repeatable without generating duplicate business records.

---

# 18. Data Quality Framework

The project must implement explicit quality rules.

## Dimensions

### Completeness

Example:

```text
senator_id IS NOT NULL
vote_id IS NOT NULL
```

### Uniqueness

Example:

```text
senator_id + vote_id
```

must uniquely identify an individual vote.

### Validity

Example:

```text
expense_amount >= 0
```

### Referential Integrity

Example:

```text
fact_expense.senator_id
```

must correspond to an existing senator.

### Freshness

Critical datasets should have a recorded latest source date and latest successful ingestion timestamp.

---

# 19. Data Quality Results

Create:

```text
dq_results
```

Example schema:

```text
execution_id
source_system
table_name
rule_name
rule_type
records_checked
records_failed
failure_percentage
status
execution_timestamp
```

Possible statuses:

```text
PASS
WARNING
FAIL
```

---

# 20. Incremental Processing

The project should use historical backfills followed by incremental ingestion when supported.

Create:

```text
etl_watermark
```

Possible columns:

```text
dataset
watermark_type
last_successful_value
last_successful_execution
updated_at
```

Example:

```text
votes
DATE
2026-10-18
```

Next execution:

```text
FROM 2026-10-19
```

Watermarks must only advance after successful ingestion.

---

# 21. Observability

Create:

```text
etl_execution_log
```

Required fields:

```text
execution_id
pipeline_name
dataset
start_timestamp
end_timestamp
rows_read
rows_written
status
error_message
duration_seconds
```

Optional:

```text
api_requests
retry_count
watermark_before
watermark_after
```

---

# 22. Gold Warehouse

Gold will use a dimensional model designed primarily for analytical consumption.

Example:

```text
wh_senado_gold
```

---

# 23. Core Dimensions

Initial dimensions:

```text
dim_senator
dim_party
dim_date
dim_state
dim_municipality
dim_expense_category
dim_legislative_matter
dim_committee
```

Possible later dimensions:

```text
dim_supplier
dim_program
dim_amendment_type
dim_vote_position
```

---

# 24. Core Facts

## fact_vote

Grain:

> one senator vote in one voting event

Example:

```text
vote_key
senator_key
matter_key
date_key
party_key
vote_position
```

---

## fact_expense

Grain:

> one published parliamentary expense

Example:

```text
expense_key
senator_key
expense_category_key
supplier_key
date_key
amount
document_number
```

---

## fact_amendment

Grain must be explicitly documented based on source availability.

Potential measures:

```text
authorized_amount
committed_amount
liquidated_amount
paid_amount
```

Dimensions:

```text
senator
municipality
state
date
amendment_type
```

---

## fact_legislative_activity

Potential events:

- authored matter;
- rapporteurship;
- committee participation;
- legislative action.

This fact is optional for MVP.

---

# 25. Analytical Views

The Gold layer should expose business-friendly views.

Examples:

```text
vw_senator_profile
vw_senator_expense_summary
vw_senator_vote_summary
vw_senator_amendment_summary

vw_expense_peer_comparison
vw_expense_historical_baseline

vw_vote_similarity
vw_party_vote_alignment

vw_municipality_amendment_summary
vw_state_amendment_summary

vw_amendment_execution
```

---

# 26. AI Serving Views

Create curated analytical views specifically for AI.

Examples:

```text
ai_senator_profile
ai_expense_analysis
ai_vote_analysis
ai_amendment_analysis
ai_municipality_funding
```

These views should prioritize:

- descriptive column names;
- documented fields;
- controlled joins;
- canonical metrics;
- simplified schemas;
- reduced ambiguity.

The AI Agent should not receive unrestricted access to all Bronze and Silver structures.

---

# 27. Canonical Business Metrics

A shared metric catalogue must be created.

Potential metrics:

## Expenses

```text
CEAPS Total
CEAPS Monthly Average
CEAPS YoY Growth
CEAPS YTD Growth
CEAPS Historical Median
CEAPS Peer Percentile
Category Share
```

---

## Voting

```text
Voting Participation Rate
Vote Alignment Rate
Party Alignment Rate
Shared Vote Count
```

---

## Amendments

```text
Authorized Amount
Committed Amount
Liquidated Amount
Paid Amount
Payment Rate
Municipal Amendment Per Capita
```

---

# 28. Vote Similarity

One analytical feature will calculate senator-to-senator voting similarity.

Initial definition:

```text
Vote Similarity =
Same Votes
/
Votes Where Both Senators Participated
```

Requirements:

- abstentions must have documented treatment;
- absences must not automatically count as disagreement;
- minimum shared-vote threshold should be defined;
- selected periods must be comparable.

The metric must not be presented as definitive proof of political alliance.

---

# 29. Expense Anomaly Analysis

The platform should support detecting statistically unusual expenses.

Possible approaches:

### MVP

Historical percentile:

```text
P95 senator/category
```

### Alternative

Robust Z-score using:

```text
median
MAD
```

### Later

Isolation Forest or similar anomaly model.

Output should communicate:

```text
statistically unusual
```

not:

```text
fraudulent
suspicious
illegal
```

unless supported by an external authoritative determination.

---

# 30. Power BI Product

The primary BI experience should tell a story rather than act as a generic dashboard.

Initial pages:

## Overview

National-level summary of:

- senators;
- voting activity;
- expenses;
- amendments.

## Senator Profile

Displays:

- political profile;
- voting participation;
- expense composition;
- historical expenditure;
- amendment allocation;
- major analytical observations.

## Voting Networks

Explores:

- senator similarity;
- party alignment;
- voting clusters.

## Public Spending

Explores:

- CEAPS;
- expense categories;
- trends;
- peer comparisons;
- statistical anomalies.

## Amendment Map

Explores:

- municipality funding;
- geographical distribution;
- parliamentarian attribution;
- amendment execution.

---

# 31. Power BI Interaction Principles

The report should avoid excessive filtering.

The product should favor:

- guided questions;
- annotations;
- narrative visuals;
- comparison;
- contextual benchmarks;
- drill-through.

Every important visual should identify:

- data source;
- period;
- metric definition where necessary.

---

# 32. Fabric Semantic Model

The semantic model should centralize:

- relationships;
- measures;
- hierarchies;
- business descriptions;
- formatting;
- KPI definitions.

Example measures:

```text
[Total CEAPS]
[CEAPS YoY]
[Voting Participation %]
[Vote Alignment %]
[Authorized Amendments]
[Paid Amendments]
[Amendment Payment %]
```

The semantic model must remain consistent with the AI analytical layer.

---

# 33. Fabric Data Agent

The Fabric Data Agent will provide governed conversational analysis over selected Fabric analytical resources.

The agent should primarily access:

```text
Gold analytical views
AI serving views
```

It should not directly interpret raw Bronze data.

---

# 34. Agent Questions

Expected questions include:

```text
Which senators spent the most through CEAPS in 2026?

How has Senator X's spending changed since 2023?

Which spending categories explain Senator X's increase?

Which senators most frequently vote with Senator X?

Which municipalities received the most paid amendments?

How much amendment funding has municipality X received?

Which senators have expenses above their own historical P95?
```

---

# 35. Agent Instructions

The Data Agent must receive domain-specific instructions.

Examples:

```text
CEAPS means Cota para o Exercício da Atividade Parlamentar.
```

```text
Differentiate authorized, committed, liquidated, and paid amendment values.
```

```text
Do not interpret statistical anomalies as evidence of misconduct.
```

```text
Voting similarity does not necessarily imply ideological or political coordination.
```

```text
When making year-over-year comparisons, use equivalent periods when the current year is incomplete.
```

---

# 36. LangGraph Layer

LangGraph will sit above the Fabric Data Agent as an orchestration layer.

Its role is not merely NL-to-SQL.

Responsibilities:

- classify analytical intent;
- plan multi-step queries;
- select tools;
- retrieve data;
- call deterministic analytical functions;
- combine results;
- explain metrics;
- expose provenance;
- generate final responses.

---

# 37. Agent Tool Model

Potential tools:

```text
query_fabric_agent()
get_senator_profile()
get_expense_history()
get_expense_peer_percentile()
get_vote_similarity()
get_amendment_distribution()
get_municipality_summary()
get_metric_definition()
get_data_provenance()
```

Where possible, deterministic calculations should be performed by data tools rather than generated directly by the LLM.

---

# 38. LangGraph Flow

```text
User Question
      │
      ▼
Intent Router
      │
      ▼
Planner
      │
      ├── Simple analytical query
      │       ▼
      │   Fabric Data Agent
      │
      ├── Deterministic metric
      │       ▼
      │   Analytical Tool
      │
      ├── Methodology question
      │       ▼
      │   Metadata Tool
      │
      └── Complex question
              ▼
          Multiple tools
              │
              ▼
            Synthesis
              │
              ▼
          Guardrail Check
              │
              ▼
        Answer + Provenance
```

---

# 39. Agent Provenance

Important analytical answers should expose provenance where possible.

Example:

```text
Source:
Portal da Transparência

Analytical object:
vw_senator_amendment_summary

Metric:
SUM(paid_amount)

Filters:
Senator = X
Period = 2023–2026

Last refresh:
2026-10-19
```

Users should be able to ask:

```text
How was this calculated?
```

and receive the underlying methodology.

---

# 40. Political Analysis Guardrails

The AI system must distinguish:

- source fact;
- calculated metric;
- statistical observation;
- model inference.

Examples:

```text
High spending ≠ misuse
```

```text
Statistical anomaly ≠ fraud
```

```text
Voting similarity ≠ political alliance
```

```text
Absence ≠ negligence
```

```text
Authorized amendment ≠ delivered public project
```

```text
Payment ≠ evidence of impact
```

AI-generated narratives should use neutral language.

---

# 41. Source Transparency

Every analytical experience should make source attribution easy to locate.

Primary sources:

```text
Senado Federal
Portal da Transparência
```

Validation/reference source:

```text
Base dos Dados
```

Where applicable, additional demographic enrichment may use authoritative sources such as IBGE.

---

# 42. Reconciliation

A reconciliation framework should compare key aggregates against Base dos Dados or source totals.

Example tests:

```text
count(senators)

count(votes)

count(expenses)

sum(expense_amount)

count(amendments)

sum(paid_amount)
```

Discrepancies must be documented rather than silently ignored.

---

# 43. Security

Because the data is public, the platform does not require complex row-level user permissions for the MVP.

However, the architecture should still follow good practice:

- API secrets outside notebook source;
- least-privilege service identities;
- controlled Fabric workspace access;
- no secret keys committed to Git;
- AI access restricted to curated analytical resources.

---

# 44. CI/CD

Target environments:

```text
DEV
TEST
PROD
```

The initial portfolio implementation may use only DEV if Fabric capacity or time constraints exist, but the design must document the target deployment model.

CI/CD should consider:

- Git integration;
- deployment pipelines;
- environment-specific configuration;
- source-control versioning;
- secrets outside repository;
- post-deployment data population.

---

# 45. Repository Structure

```text
senado-fabric-platform/
│
├── README.md
│
├── architecture/
│   ├── solution-architecture.png
│   ├── data-flow.png
│   └── agent-architecture.png
│
├── pipelines/
│
├── notebooks/
│   ├── framework/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── sql/
│   ├── dimensions/
│   ├── facts/
│   ├── analytical_views/
│   └── ai_views/
│
├── semantic-model/
│
├── powerbi/
│
├── agent/
│   ├── fabric-agent/
│   └── langgraph/
│
├── tests/
│
├── config/
│
└── docs/
    ├── PRD.md
    ├── data_dictionary.md
    ├── metric_catalog.md
    ├── data_quality.md
    ├── orchestration.md
    └── decisions/
```

---

# 46. Architecture Decision Records

Recommended ADRs:

```text
ADR-001-official-government-sources.md
ADR-002-medallion-architecture.md
ADR-003-separate-bronze-silver-lakehouses.md
ADR-004-gold-fabric-warehouse.md
ADR-005-metadata-driven-ingestion.md
ADR-006-bulk-plus-api-transparency-data.md
ADR-007-fabric-data-agent.md
ADR-008-langgraph-orchestration.md
ADR-009-no-eventstream.md
ADR-010-ai-serving-views.md
```

---

# 47. MVP Scope

Because the project has a short delivery window, the MVP should prioritize an end-to-end vertical slice.

## Must Have

### Engineering

- one working Fabric Data Factory orchestration;
- official government API ingestion;
- Bronze Lakehouse;
- Silver Lakehouse;
- PySpark transformations;
- Gold Warehouse;
- incremental strategy for at least one source;
- data-quality checks;
- audit logs.

### Data

At minimum:

- senators;
- votes;
- CEAPS;
- amendments;
- municipalities.

### Analytics

At minimum:

- senator profile;
- expense analysis;
- voting comparison;
- amendment geography.

### AI

At minimum:

- Fabric Data Agent;
- curated AI analytical views;
- several validated question patterns.

### Portfolio

- GitHub repository;
- architecture diagram;
- README;
- source documentation;
- methodology;
- screenshots/demo.

---

# 48. Should Have

If time permits:

- metadata-driven ingestion across every source;
- LangGraph orchestration;
- deterministic agent tools;
- vote-similarity matrix;
- peer percentile calculations;
- anomaly detection;
- reconciliation against Base dos Dados;
- AI provenance display.

---

# 49. Could Have

Future extensions:

- automated weekly Senate summary;
- automatic social-media content generation;
- downloadable senator profile;
- natural-language chart generation;
- vector search over legislative documents;
- integration with legislation text;
- ontology layer;
- advanced entity resolution;
- more sophisticated anomaly models;
- public web application.

---

# 50. Explicitly Deferred

Not in initial scope:

- Eventstream;
- Eventhouse;
- Fabric Activator;
- Kafka;
- real-time event detection;
- production notification infrastructure.

These components should only be added if a genuine real-time requirement appears later.

---

# 51. MVP Delivery Sequence

## Phase 1 — Foundation

1. Create Fabric workspace.
2. Create Bronze Lakehouse.
3. Create Silver Lakehouse.
4. Create Gold Warehouse.
5. Define naming convention.
6. Configure Git integration.

---

## Phase 2 — Ingestion

1. Senado Legislative API.
2. Senado Administrative API.
3. Portal da Transparência.
4. Metadata table.
5. API pagination.
6. Audit logs.
7. Watermarks.

---

## Phase 3 — Transformation

1. Bronze normalization.
2. Senator entity.
3. Municipality entity.
4. Vote normalization.
5. CEAPS normalization.
6. Amendment normalization.
7. Data quality.

---

## Phase 4 — Gold

1. Dimensions.
2. Fact tables.
3. Analytical views.
4. Metric catalogue.
5. AI serving views.

---

## Phase 5 — Analytics

1. Semantic model.
2. Measures.
3. Senator profile.
4. Voting analysis.
5. Expense analysis.
6. Amendment map.

---

## Phase 6 — AI

1. Configure Fabric Data Agent.
2. Add source descriptions.
3. Add instructions.
4. Add example queries.
5. Validate generated answers.
6. Add LangGraph if schedule permits.
7. Add deterministic analytical tools.

---

## Phase 7 — Portfolio

1. Architecture diagram.
2. Data flow diagram.
3. Agent diagram.
4. Technical README.
5. ADRs.
6. Screenshots.
7. Demo recording.
8. LinkedIn/Data Glow Up publication.

---

# 52. Success Criteria

The project is considered successful when a reviewer can:

### Data Engineering

Follow:

```text
official source
→ ingestion
→ Bronze
→ Silver
→ Gold
```

and understand how failures, incremental processing, and data quality are handled.

### Analytics

Select a senator and reliably analyze:

- votes;
- expenses;
- amendments.

### AI

Ask:

```text
How much did Senator X spend in 2026?
```

and receive a result consistent with the BI model.

The user should also be able to ask:

```text
How was that calculated?
```

and obtain the methodology and source context.

### Portfolio

A technical reviewer should be able to identify demonstrated experience with:

- Microsoft Fabric;
- Data Factory;
- OneLake;
- Lakehouse;
- Delta;
- Spark;
- Warehouse;
- Power BI;
- dimensional modeling;
- API integration;
- data quality;
- observability;
- Generative AI;
- agent orchestration.

---

# 53. Main Portfolio Narrative

The final project should be describable as:

> Senado 360 is an end-to-end public-data platform built on Microsoft Fabric using official Brazilian government APIs. Fabric Data Factory performs metadata-driven and incremental ingestion into a medallion architecture using Bronze and Silver Lakehouses. PySpark standardizes, validates, and conforms the data before analytical facts and dimensions are published to a Fabric Warehouse. A shared business layer powers a Power BI semantic model and curated AI-serving views. Fabric Data Agent provides governed conversational analytics, while LangGraph can orchestrate multi-step reasoning, deterministic analytical tools, methodology retrieval, and source provenance.

---

# 54. Key Architectural Principle

The defining architecture of the project is:

```text
Official APIs
      ↓
Data Factory
      ↓
Bronze Lakehouse
      ↓
Silver Lakehouse
      ↓
Gold Warehouse
      ↓
Governed Analytical Layer
      ↓
 ┌───────────────┬─────────────────┐
 ↓               ↓                 ↓
Power BI   Fabric Data Agent    LangGraph
```

The portfolio should demonstrate not only that the system produces useful analytics, but that the platform is **reproducible, observable, governed, explainable, and architecturally coherent**.