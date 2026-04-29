{{ config(materialized='table') }}

-- Mart: orders fact table. Consumer-facing model used by BI and reverse-ETL.

with enriched as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_id,
    customer_id,
    customer_email,
    customer_name,
    order_status,
    currency,
    order_total,
    item_count,
    items_total,
    -- Derived metrics
    case
        when order_total > 0
            then round(items_total / order_total, 4)
        else null
    end                                     as items_to_total_ratio,
    ordered_at,
    date(ordered_at)                        as ordered_date,
    updated_at
from enriched
