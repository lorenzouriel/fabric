## Summary

<!-- What does this PR change, and why? -->

Backlog: <!-- e.g. E2.S4 -->

## Type of change

- [ ] Infrastructure (Terraform)
- [ ] Ingestion (pipelines / Bronze)
- [ ] Transformation (notebooks / Silver)
- [ ] Gold (dimensions, facts, views)
- [ ] Semantic model / Power BI
- [ ] AI (Data Agent / LangGraph)
- [ ] Docs / ADR
- [ ] Other

## Layers / items affected

<!-- e.g. lh_senado_silver.senator, wh_senado_gold.fact_vote, pl_master_ingestion -->

## How it was tested

<!-- Commands run, notebook runs, row counts, DQ results, screenshots. -->

## Checklist

- [ ] Follows [naming conventions](../docs/naming_conventions.md)
- [ ] No secrets, keys or `*.tfvars` committed
- [ ] **Terraform:** `terraform fmt` + `validate` pass; plan output reviewed and pasted below (no unexpected destroy/replace)
- [ ] **Data:** re-running is idempotent (no duplicate business records)
- [ ] **Data:** DQ rules added/updated for new tables or columns
- [ ] **Metrics:** metric changes reflected in `docs/metric_catalog.md` and kept consistent between the semantic model and AI views
- [ ] **AI:** outputs keep neutral language (no misconduct/fraud inferences)
- [ ] Docs / data dictionary / ADR updated where relevant
- [ ] Backlog checkboxes updated

<details>
<summary>Terraform plan (if applicable)</summary>

```text

```

</details>
