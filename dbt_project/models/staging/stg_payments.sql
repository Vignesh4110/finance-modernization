/*
    Staging model for Payment Transactions (PAYTRAN)
*/

with source as (
    select * from {{ ref('paytran') }}
),

cleaned as (
    select
        payment_id,
        customer_id,
        invoice_reference,
        payment_date,
        coalesce(payment_amount, 0) as payment_amount,
        trim(cast(payment_method as varchar)) as payment_method_code,
        case trim(cast(payment_method as varchar))
            when 'CK' then 'Check'
            when 'AC' then 'ACH'
            when 'WR' then 'Wire'
            when 'CC' then 'Credit Card'
            else 'Other'
        end as payment_method,
        trim(cast(check_number as varchar)) as check_number,
        trim(cast(bank_reference as varchar)) as bank_reference,
        trim(cast(remittance_name as varchar)) as remittance_name,
        trim(cast(applied_flag as varchar)) = 'Y' as is_applied,
        applied_date,
        coalesce(applied_amount, 0) as applied_amount,
        coalesce(unapplied_amount, 0) as unapplied_amount,
        trim(cast(payment_type as varchar)) as payment_type_code,
        case trim(cast(payment_type as varchar))
            when 'PM' then 'Payment'
            when 'RF' then 'Refund'
            when 'AD' then 'Adjustment'
            else 'Other'
        end as payment_type,
        trim(cast(status as varchar)) as status_code,
        case trim(cast(status as varchar))
            when 'RV' then 'Received'
            when 'AP' then 'Applied'
            when 'RJ' then 'Rejected'
            when 'RT' then 'Returned'
            else 'Unknown'
        end as status,
        trim(cast(batch_id as varchar)) as batch_id,
        case when invoice_reference is null or invoice_reference = 0 then true else false end as is_missing_invoice_ref,
        created_date,
        updated_date,
        trim(cast(updated_by as varchar)) as updated_by
    from source
    where payment_id is not null
)

select * from cleaned