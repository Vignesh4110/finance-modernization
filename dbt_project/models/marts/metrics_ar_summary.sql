/*
    Metrics: AR Summary
    
    Key AR metrics for executive dashboard
*/

with ar_aging as (
    select * from {{ ref('fct_ar_aging') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
)

select
    -- Report Date
    current_date as report_date,
    
    -- AR Totals
    count(distinct ar_aging.invoice_number) as open_invoice_count,
    sum(ar_aging.current_balance) as total_ar_balance,
    
    -- Aging Buckets
    sum(ar_aging.current_amount) as current_bucket,
    sum(ar_aging.amount_1_30) as bucket_1_30,
    sum(ar_aging.amount_31_60) as bucket_31_60,
    sum(ar_aging.amount_61_90) as bucket_61_90,
    sum(ar_aging.amount_over_90) as bucket_over_90,
    
    -- Aging Percentages
    round(sum(ar_aging.current_amount) / nullif(sum(ar_aging.current_balance), 0) * 100, 1) as pct_current,
    round(sum(ar_aging.amount_over_90) / nullif(sum(ar_aging.current_balance), 0) * 100, 1) as pct_over_90,
    
    -- Disputed
    sum(case when ar_aging.is_disputed then ar_aging.current_balance else 0 end) as disputed_amount,
    count(case when ar_aging.is_disputed then 1 end) as disputed_count,
    
    -- DSO Calculation (simplified)
    round(
        sum(ar_aging.current_balance) / 
        nullif((select sum(invoice_amount) / 365 from invoices where invoice_date >= current_date - 365), 0)
    , 1) as days_sales_outstanding,
    
    -- Collections Metrics
    (select count(*) from payments where is_applied = false) as unapplied_payment_count,
    (select sum(payment_amount) from payments where is_applied = false) as unapplied_payment_amount,
    
    -- Customer Concentration
    count(distinct ar_aging.customer_id) as customers_with_open_ar

from ar_aging