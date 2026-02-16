/*
    Staging model for GL Journal Entries (GLJRN)
*/

with source as (
    select * from {{ ref('gljrn') }}
),

cleaned as (
    select
        journal_id,
        line_number,
        post_date,
        period as fiscal_period,
        fiscal_year,
        trim(cast(gl_account as varchar)) as gl_account,
        case trim(cast(gl_account as varchar))
            when '1100' then 'Cash'
            when '1200' then 'Accounts Receivable'
            when '1210' then 'Allowance for Doubtful Accounts'
            when '2100' then 'Accounts Payable'
            when '4100' then 'Product Revenue'
            when '4200' then 'Service Revenue'
            when '6100' then 'Bad Debt Expense'
            else 'Other'
        end as gl_account_name,
        trim(cast(department as varchar)) as department,
        trim(cast(project as varchar)) as project,
        coalesce(debit_amount, 0) as debit_amount,
        coalesce(credit_amount, 0) as credit_amount,
        coalesce(debit_amount, 0) - coalesce(credit_amount, 0) as net_amount,
        trim(cast(description as varchar)) as description,
        trim(cast(reference as varchar)) as reference,
        trim(cast(source as varchar)) as source_code,
        case trim(cast(source as varchar))
            when 'AR' then 'Accounts Receivable'
            when 'AP' then 'Accounts Payable'
            when 'GL' then 'General Ledger'
            when 'PR' then 'Payroll'
            else 'Other'
        end as source_name,
        trim(cast(document_type as varchar)) as document_type,
        trim(cast(status as varchar)) as status_code,
        case trim(cast(status as varchar))
            when 'P' then 'Posted'
            when 'R' then 'Reversed'
            when 'E' then 'Error'
            else 'Unknown'
        end as status,
        trim(cast(reversal_flag as varchar)) = 'Y' as is_reversal,
        reversal_journal,
        created_date,
        updated_date,
        trim(cast(updated_by as varchar)) as updated_by
    from source
    where journal_id is not null
)

select * from cleaned