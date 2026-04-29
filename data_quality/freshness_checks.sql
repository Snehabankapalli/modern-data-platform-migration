-- freshness_checks.sql
--
-- Lightweight post-load freshness assertions executed by the Airflow DAG
-- after dbt has finished. Any row returned with status='error' will be
-- treated as a failed check by the ON_ERROR_STOP=1 wrapper combined with
-- the RAISE EXCEPTION block at the end.
--
-- Thresholds intentionally mirror the dbt source freshness config in
-- dbt/tests/schema.yml so behaviour is consistent across both layers.

with checks as (
    select
        'raw.legacy_customers'                                  as table_name,
        max(_ingested_at)                                       as last_ingested_at,
        extract(epoch from (now() - max(_ingested_at))) / 3600  as hours_since_load,
        24                                                      as error_after_hours
    from raw.legacy_customers

    union all

    select
        'raw.legacy_orders',
        max(_ingested_at),
        extract(epoch from (now() - max(_ingested_at))) / 3600,
        6
    from raw.legacy_orders

    union all

    select
        'raw.legacy_order_items',
        max(_ingested_at),
        extract(epoch from (now() - max(_ingested_at))) / 3600,
        6
    from raw.legacy_order_items
),

graded as (
    select
        table_name,
        last_ingested_at,
        round(hours_since_load::numeric, 2) as hours_since_load,
        error_after_hours,
        case
            when last_ingested_at is null                       then 'error'
            when hours_since_load > error_after_hours           then 'error'
            when hours_since_load > error_after_hours * 0.5     then 'warn'
            else 'ok'
        end as status
    from checks
)

select * from graded order by status desc, table_name;

-- Fail loudly if any source is in error state.
do $$
declare
    failed_count integer;
begin
    select count(*) into failed_count
    from (
        select
            case
                when max(_ingested_at) is null then 1
                when extract(epoch from (now() - max(_ingested_at))) / 3600 > 24 then 1
                else 0
            end as is_failed
        from raw.legacy_customers
        union all
        select case
            when max(_ingested_at) is null then 1
            when extract(epoch from (now() - max(_ingested_at))) / 3600 > 6 then 1
            else 0 end
        from raw.legacy_orders
        union all
        select case
            when max(_ingested_at) is null then 1
            when extract(epoch from (now() - max(_ingested_at))) / 3600 > 6 then 1
            else 0 end
        from raw.legacy_order_items
    ) f
    where is_failed = 1;

    if failed_count > 0 then
        raise exception 'Freshness check failed for % source table(s)', failed_count;
    end if;
end
$$;
