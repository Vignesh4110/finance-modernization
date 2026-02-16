/*
    Staging model for AR Invoice Master (ARMAS)
*/

with source as (
    select * from {{ ref('armas') }}
),

cleaned as (
    select
        invoice_number,
        customer_id,
        invoice_date,
        due_date,
        ship_date,
        trim(cast(po_number as varchar)) as po_number,
        trim(cast(reference1 as varchar)) as reference1,
        coalesce(invoice_amount, 0) as invoice_amount,
        coalesce(tax_amount, 0) as tax_amount,
        coalesce(freight_amount, 0) as freight_amount,
        coalesce(discount_amount, 0) as discount_amount,
        coalesce(invoice_amount, 0) + coalesce(tax_amount, 0) + coalesce(freight_amount, 0) as gross_amount,
        coalesce(amount_paid, 0) as amount_paid,
        coalesce(current_balance, 0) as current_balance,
        trim(cast(status as varchar)) as status_code,
        case trim(cast(status as varchar))
            when 'OP' then 'Open'
            when 'PD' then 'Paid'
            when 'PP' then 'Partial Payment'
            when 'DP' then 'Disputed'
            when 'WO' then 'Written Off'
            else 'Unknown'
        end as status,
        trim(cast(hold_flag as varchar)) = 'Y' as is_on_hold,
        trim(cast(dispute_flag as varchar)) = 'Y' as is_disputed,
        trim(cast(dispute_reason as varchar)) as dispute_reason_code,
        payment_terms as payment_terms_days,
        trim(cast(document_type as varchar)) as document_type,
        trim(cast(division as varchar)) as division,
        trim(cast(gl_account as varchar)) as gl_account,
        gl_post_date,
        trim(cast(gl_posted_flag as varchar)) = 'Y' as is_gl_posted,
        case 
            when coalesce(current_balance, 0) <= 0 then 'Paid'
            when due_date >= current_date then 'Current'
            when current_date - due_date between 1 and 30 then '1-30 Days'
            when current_date - due_date between 31 and 60 then '31-60 Days'
            when current_date - due_date between 61 and 90 then '61-90 Days'
            else '90+ Days'
        end as aging_bucket,
        case 
            when due_date >= current_date then 0
            else cast(current_date - due_date as integer)
        end as days_past_due,
        case when coalesce(current_balance, 0) > 0 then true else false end as is_open,
        created_date,
        updated_date,
        trim(cast(updated_by as varchar)) as updated_by
    from source
    where invoice_number is not null
)

select * from cleaned