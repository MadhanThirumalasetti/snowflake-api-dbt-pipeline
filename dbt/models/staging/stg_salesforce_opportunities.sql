-- stg_salesforce_opportunities.sql
-- Staging model: clean and type-cast raw Salesforce opportunity records
-- from the JSON external stage before downstream transformation.

with source as (

    select
        parse_json($1) as payload

    from {{ source('raw', 'salesforce_opportunities_raw') }}

),

renamed as (

    select
        payload:Id::varchar(18)               as opportunity_id,
        payload:Name::varchar(255)            as opportunity_name,
        payload:StageName::varchar(100)       as stage_name,
        payload:Amount::float                 as amount_usd,
        payload:CloseDate::date               as close_date,
        payload:AccountId::varchar(18)        as account_id,
        payload:OwnerId::varchar(18)          as owner_id,

        convert_timezone('UTC', payload:CreatedDate::timestamp_tz)      as created_at,
        convert_timezone('UTC', payload:LastModifiedDate::timestamp_tz) as updated_at,

        current_timestamp()                   as _loaded_at,
        '{{ invocation_id }}'                 as _dbt_run_id

    from source

),

cleaned as (

    select *
    from renamed
    where opportunity_id is not null
      and amount_usd > 0
      and stage_name not ilike '%deleted%'

)

select * from cleaned
