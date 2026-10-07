-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    amount_cents / 100.0 as order_amount_usd,
    currency,
    created_at as order_timestamp
from {{ source('ecom', 'orders') }}
