-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('raw_crm', 'customers') }}
)
select
    id as customer_id,
    acc_no as account_id, -- Refactored from customer_account_id in PR #402
    email,
    tier,
    created_at
from source
