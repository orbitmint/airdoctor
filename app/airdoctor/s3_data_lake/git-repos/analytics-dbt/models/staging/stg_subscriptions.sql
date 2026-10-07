-- models/staging/stg_subscriptions.sql
select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price_usd,
    status,
    started_at,
    canceled_at
from {{ source('billing', 'subscriptions') }}
