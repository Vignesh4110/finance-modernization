/*
    Dimension: Customers
    
    Customer dimension with aggregated AR metrics
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

invoice_metrics as (
    select
        customer_id,
        count(*) as total_invoices,
        sum(invoice_amount) as lifetime_invoice_amount,
        sum(current_balance) as total_ar_balance,
        sum(case when is_open then 1 else 0 end) as open_invoice_count,
        sum(case when aging_bucket = '90+ Days' then current_balance else 0 end) as over_90_balance,
        min(invoice_date) as first_invoice_date,
        max(invoice_date) as last_invoice_date
    from {{ ref('stg_invoices') }}
    group by customer_id
),

payment_metrics as (
    select
        customer_id,
        count(*) as total_payments,
        sum(payment_amount) as lifetime_payment_amount,
        avg(payment_amount) as avg_payment_amount,
        max(payment_date) as last_payment_date
    from {{ ref('stg_payments') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.contact_name,
    c.email,
    c.address_line1,
    c.city,
    c.state,
    c.zip_code,
    c.region_code,
    c.region_name,
    c.industry_code,
    c.segment_code,
    c.segment_name,
    c.credit_limit,
    c.credit_used,
    c.credit_available,
    c.payment_terms_days,
    c.credit_status,
    c.account_status,
    c.is_active,
    c.is_credit_active,
    
    -- Invoice Metrics
    coalesce(i.total_invoices, 0) as total_invoices,
    coalesce(i.lifetime_invoice_amount, 0) as lifetime_invoice_amount,
    coalesce(i.total_ar_balance, 0) as total_ar_balance,
    coalesce(i.open_invoice_count, 0) as open_invoice_count,
    coalesce(i.over_90_balance, 0) as over_90_balance,
    i.first_invoice_date,
    i.last_invoice_date,
    
    -- Payment Metrics
    coalesce(p.total_payments, 0) as total_payments,
    coalesce(p.lifetime_payment_amount, 0) as lifetime_payment_amount,
    coalesce(p.avg_payment_amount, 0) as avg_payment_amount,
    p.last_payment_date,
    
    -- Derived Metrics
    case 
        when coalesce(i.lifetime_invoice_amount, 0) > 0 
        then round(coalesce(p.lifetime_payment_amount, 0) / i.lifetime_invoice_amount * 100, 2)
        else 0 
    end as payment_rate_pct,
    
    case 
        when coalesce(i.total_ar_balance, 0) > c.credit_limit then true 
        else false 
    end as is_over_credit_limit,
    
    -- Risk Flag
    case
        when coalesce(i.over_90_balance, 0) > 0 then 'High Risk'
        when coalesce(i.total_ar_balance, 0) > c.credit_limit * 0.8 then 'Medium Risk'
        else 'Low Risk'
    end as risk_category,
    
    c.created_date,
    c.updated_date

from customers c
left join invoice_metrics i on c.customer_id = i.customer_id
left join payment_metrics p on c.customer_id = p.customer_id