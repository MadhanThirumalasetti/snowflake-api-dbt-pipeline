-- fct_sales_pipeline.sql
-- Fact table: sales pipeline metrics per opportunity with period-over-period
-- window functions consumed by analytics and ML feature engineering teams.

with opportunities as (

    select * from {{ ref('stg_salesforce_opportunities') }}

),

accounts as (

    select * from {{ ref('stg_salesforce_accounts') }}

),

enriched as (

    select
        o.opportunity_id,
        o.opportunity_name,
        o.stage_name,
        o.amount_usd,
        o.close_date,
        o.account_id,
        o.owner_id,
        o.created_at,
        o.updated_at,

        a.account_name,
        a.industry,
        a.region,

        -- Days in current stage (useful for ML churn/stall features)
        datediff('day', o.created_at, current_timestamp()) as days_in_pipeline,

        -- Running total of pipeline value per account
        sum(o.amount_usd) over (
            partition by o.account_id
            order by o.created_at
            rows between unbounded preceding and current row
        ) as running_account_pipeline_usd,

        -- Rank opportunities by value within each owner
        row_number() over (
            partition by o.owner_id
            order by o.amount_usd desc
        ) as rank_by_owner,

        -- Quarter-over-quarter amount comparison
        lag(o.amount_usd, 1) over (
            partition by o.account_id
            order by o.close_date
        ) as prev_period_amount_usd,

        o.amount_usd - lag(o.amount_usd, 1) over (
            partition by o.account_id
            order by o.close_date
        ) as amount_delta_usd

    from opportunities o
    left join accounts a
        on o.account_id = a.account_id

)

select * from enriched
