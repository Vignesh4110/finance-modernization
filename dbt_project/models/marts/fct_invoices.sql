/*
    Fact table for invoices with customer enrichment
*/
with invoices as (
    select * from {{ ref('stg_invoices') }}
),

customers as (
    select 
        customer_id,
        customer_name,
        segment_name,
        region_name,
        credit_limit,
        payment_terms_days,
        risk_category
    from {{ ref('dim_customers') }}
),

final as (
    select
        i.invoice_number,
        i.customer_id,
        c.customer_name,
        c.segment_name,
        c.region_name,
        c.risk_category,
        i.invoice_date,
        i.due_date,
        i.ship_date,
        i.po_number,
        i.reference1,
        i.invoice_amount,
        i.tax_amount,
        i.freight_amount,
        i.discount_amount,
        i.gross_amount,
        i.amount_paid,
        i.current_balance,
        i.status_code,
        i.status,
        i.is_on_hold,
        i.is_disputed,
        i.dispute_reason_code,
        i.payment_terms_days,
        i.document_type,
        i.division,
        i.gl_account,
        i.gl_post_date,
        i.is_gl_posted,
        i.aging_bucket,
        i.days_past_due as days_outstanding,
        i.is_open,
        i.created_date,
        i.updated_date,
        i.updated_by
    from invoices i
    left join customers c on i.customer_id = c.customer_id
)

select * from final
