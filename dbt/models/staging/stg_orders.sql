{{ config(materialized='view') }}

-- Staging model for orders extracted from the legacy OLTP system.

with source as (
    select *
    from {{ source('legacy', 'legacy_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        cast(order_status as varchar)       as order_status,
        cast(order_total as numeric(18, 2)) as order_total,
        cast(currency as varchar)           as currency,
        cast(ordered_at as timestamp)       as ordered_at,
        cast(updated_at as timestamp)       as updated_at,
        _ingested_at,
        _source_system
    from source
    where order_id is not null
)

select * from renamed
