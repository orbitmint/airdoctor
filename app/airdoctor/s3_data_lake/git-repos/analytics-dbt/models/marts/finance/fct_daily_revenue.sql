-- models/marts/finance/fct_daily_revenue.sql
{{ config(materialized='table') }}
select
    date_trunc('day', order_timestamp)::date as order_date,
    count(distinct order_id) as total_orders,
    sum(order_amount_usd) as gross_revenue_usd
from {{ ref('stg_orders') }}
group by 1
