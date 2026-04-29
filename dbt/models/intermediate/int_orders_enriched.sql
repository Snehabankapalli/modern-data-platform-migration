{{ config(materialized='view') }}

-- Intermediate model: orders enriched with customer attributes and
-- aggregated line-item totals. Pure business logic, no source-system
-- specifics.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

items as (
    select
        order_id,
        sum(quantity)    as item_count,
        sum(line_total)  as items_total
    from {{ ref('stg_order_items') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    c.email                                 as customer_email,
    c.first_name || ' ' || c.last_name      as customer_name,
    o.order_status,
    o.currency,
    o.order_total,
    coalesce(i.item_count, 0)               as item_count,
    coalesce(i.items_total, 0)              as items_total,
    o.ordered_at,
    o.updated_at
from orders o
left join customers c on c.customer_id = o.customer_id
left join items     i on i.order_id    = o.order_id
