-- models/marts/finance/fct_customer_churn_daily.sql
{{ config(
    materialized='table',
    partition_by={'field': 'churn_date', 'data_type': 'date'},
    cluster_by=['customer_id']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
)

select
    s.subscription_id,
    c.customer_id,
    c.customer_account_id,  -- BREAKING CHANGE: upstream renamed to account_id
    s.plan_name,
    s.monthly_price_usd,
    s.canceled_at::date as churn_date
from subscriptions s
inner join customers c on s.customer_id = c.customer_id
where s.status = 'canceled'
