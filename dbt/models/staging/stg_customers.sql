{{ config(materialized='view') }}

-- Staging model for customers extracted from the legacy OLTP system.
-- Responsibilities: typing, renaming, light cleanup. No business logic.

with source as (
    select *
    from {{ source('legacy', 'legacy_customers') }}
),

renamed as (
    select
        id                              as customer_id,
        cast(email as varchar)          as email,
        trim(first_name)                as first_name,
        trim(last_name)                 as last_name,
        cast(created_at as timestamp)   as created_at,
        cast(updated_at as timestamp)   as updated_at,
        _ingested_at,
        _source_system
    from source
    where id is not null
)

select * from renamed
