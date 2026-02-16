/*
    Fact: AR Aging
    
    Open receivables with aging buckets for collections analysis.
    This replaces the nightly ARAGIN01 spool file report.
*/

with open_invoices as (
    select * from {{ ref('stg_invoices') }}
    where is_open = true
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    -- Invoice Details
    i.invoice_number,
    i.customer_id,
    c.customer_name,
    c.segment_name,
    c.region_name,
    c.credit_limit,
    c.payment_terms_days,
    
    -- Invoice Info
    i.invoice_date,
    i.due_date,
    i.invoice_amount,
    i.amount_paid,
    i.current_balance,
    
    -- Aging
    i.days_past_due,
    i.aging_bucket,
    
    -- Aging Bucket Amounts (for pivot/aggregation)
    case when i.aging_bucket = 'Current' then i.current_balance else 0 end as current_amount,
    case when i.aging_bucket = '1-30 Days' then i.current_balance else 0 end as amount_1_30,
    case when i.aging_bucket = '31-60 Days' then i.current_balance else 0 end as amount_31_60,
    case when i.aging_bucket = '61-90 Days' then i.current_balance else 0 end as amount_61_90,
    case when i.aging_bucket = '90+ Days' then i.current_balance else 0 end as amount_over_90,
    
    -- Status Flags
    i.status,
    i.is_on_hold,
    i.is_disputed,
    i.dispute_reason_code,
    
    -- Collection Priority Score (higher = more urgent)
    case
        when i.aging_bucket = '90+ Days' then 100
        when i.aging_bucket = '61-90 Days' then 75
        when i.aging_bucket = '31-60 Days' then 50
        when i.aging_bucket = '1-30 Days' then 25
        else 0
    end 
    + case when i.current_balance > 10000 then 20 else 0 end
    + case when i.is_disputed then -30 else 0 end
    + case when c.segment_name = 'Enterprise' then 10 else 0 end
    as collection_priority_score,
    
    -- Timestamps
    current_date as report_date

from open_invoices i
left join customers c on i.customer_id = c.customer_id

order by collection_priority_score desc, i.current_balance desc