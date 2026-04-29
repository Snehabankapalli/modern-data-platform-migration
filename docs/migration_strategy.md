# Migration Strategy

## Guiding principles

1. **Strangler fig, not big bang.** Migrate one source/domain at a time and
   let the legacy and modern stacks coexist until parity is proven.
2. **Idempotent and replayable.** Every step in the new pipeline must be
   safe to re-run for any window without producing duplicates or partial state.
3. **Contracts at the seams.** Source-team-owned data contracts are the
   commitment surface; everything else is implementation detail.
4. **Observability is non-negotiable.** No new pipeline ships without
   freshness checks, row-count assertions, and lineage metadata.

## Phases

### Phase 0 — Foundations (week 1-2)
- Stand up the warehouse and `raw`, `staging`, `intermediate`, `marts`,
  and `meta` schemas.
- Provision Airflow and dbt environments.
- Wire up secrets management and Airflow connections.
- Establish CI for dbt (compile + test on PR) and Python (lint + unit tests).

### Phase 1 — Pilot domain (week 3-5)
- Pick one low-risk domain (e.g., `customers`) and migrate it end-to-end.
- Backfill historical data, then enable incremental loads.
- Run legacy and modern pipelines in parallel; reconcile row counts and key
  metrics daily.

### Phase 2 — Expansion (week 6-12)
- Onboard remaining domains in priority order.
- Decommission legacy transformations as marts reach parity.
- Migrate downstream consumers (BI dashboards, reverse-ETL syncs).

### Phase 3 — Decommission (week 13+)
- Freeze writes to legacy transformations.
- Archive legacy artifacts (code, data) per retention policy.
- Final cutover communication and post-mortem.

## Parallel-run reconciliation

For each migrated entity we compare:

| Check                       | Tolerance               |
|-----------------------------|-------------------------|
| Daily row counts            | exact match             |
| Sum of monetary columns     | within $0.01 / day     |
| Distinct primary key counts | exact match             |
| Latest `updated_at`         | within 5 minutes        |

A domain is considered "migrated" only after 14 consecutive days of clean
reconciliation.

## Risks and mitigations

- **Schema drift in legacy sources** — caught by ingestion-time validation
  rules and dbt schema tests; alerts route to the source-owning team.
- **Silent data loss** — mitigated by row-count reconciliation and
  watermark-based incremental loads.
- **Consumer breakage** — mitigated by data contracts and a deprecation
  window for any breaking change in marts.

## Rollback

Because the legacy stack remains operational throughout Phases 1-2, rollback
is simply "point consumers back at legacy." Phase 3 is the only
non-trivial rollback point and is gated on explicit sign-off from data
consumers.
