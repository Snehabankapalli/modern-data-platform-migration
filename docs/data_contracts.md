# Data Contracts

A data contract is the explicit, versioned agreement between a producer (the
team that owns a source system or upstream model) and its consumers (anyone
who reads downstream of it). Contracts apply at two boundaries in this
platform:

1. **Legacy source -> raw landing zone** — owned by the legacy system team.
2. **Marts -> downstream consumers (BI / ML / reverse-ETL)** — owned by
   the data platform team.

## What a contract specifies

- **Schema**: column names, types, nullability, primary keys.
- **Semantics**: definition of each column in business terms.
- **SLAs**: freshness target and expected throughput.
- **Change policy**: what changes are additive (non-breaking) vs. breaking,
  and the deprecation window for breaking changes.
- **Quality guarantees**: tests run by the producer (uniqueness, referential
  integrity, accepted values).

## Versioning

Contracts use semantic versioning:

- **MAJOR** — breaking changes (column removed/renamed, type narrowed,
  semantics changed). Requires consumer migration and a 30-day deprecation
  window.
- **MINOR** — additive changes (new columns, new optional fields). Safe for
  consumers; no action required.
- **PATCH** — documentation or test changes only.

## Example contract — `marts.fct_orders` v1.0.0

| Column                | Type           | Null | Notes                              |
|-----------------------|----------------|------|------------------------------------|
| order_id              | string         | no   | PK. Stable across the order lifecycle. |
| customer_id           | string         | no   | FK to dim_customers.               |
| customer_email        | string         | yes  | May be null for guest checkouts.   |
| order_status          | string         | no   | Enum: pending/paid/shipped/cancelled/refunded. |
| currency              | string (ISO)   | no   | 3-letter code.                     |
| order_total           | numeric(18,2)  | no   | In `currency`. Source of truth.   |
| item_count            | integer        | no   | Sum of line-item quantities.       |
| items_total           | numeric(18,2)  | no   | Sum of line totals.                |
| ordered_at            | timestamp      | no   | UTC.                               |
| ordered_date          | date           | no   | Derived from ordered_at.           |
| updated_at            | timestamp      | no   | Last mutation time.                |

**SLAs**

- Freshness: < 2 hours from source mutation to mart, p95.
- Throughput: handles up to 10M orders/day without backfill.
- Availability: 99.5% during business hours.

**Quality guarantees**

- order_id is unique and not null.
- customer_id is not null and present in dim_customers.
- order_status is one of the accepted values.

## Process

1. Producer drafts the contract as a markdown file under `docs/contracts/`.
2. Consumers review and sign off in the PR.
3. Contract is versioned and published; tests in `dbt/tests/schema.yml`
   enforce it on every run.
4. Breaking changes require a new MAJOR version, a deprecation note, and a
   migration guide.
