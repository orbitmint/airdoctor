-- queries/settlements/calc_effective_take_rate.sql
SELECT 
    merchant_id, 
    currency,
    net_payout / gross_transactions AS take_rate
FROM financial.merchant_fee_ledger
WHERE batch_dt = '2026-09-29';
