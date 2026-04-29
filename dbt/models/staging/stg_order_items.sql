{{ config(materialized='view') }}

-- Staging model for order line items.

with source as (
    select *
    from {{ source('legacy', 'legacy_order_items') }}
),

renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as integer)            as quantity,
        cast(unit_price as numeric(18, 2))   as unit_price,
        cast(line_total as numeric(18, 2))   as line_total,
        cast(updated_at as timestamp)        as updated_at,
        _ingested_at,
        _source_system
    from source
    where order_item_id is not null
)

select * from renamed
