/*
    Staging model for Customer Master (CUSMAS)
*/

with source as (
    select * from {{ ref('cusmas') }}
),

cleaned as (
    select
        customer_id,
        trim(cast(customer_name as varchar)) as customer_name,
        trim(cast(contact_name as varchar)) as contact_name,
        trim(cast(email as varchar)) as email,
        trim(cast(address_line1 as varchar)) as address_line1,
        trim(cast(address_line2 as varchar)) as address_line2,
        trim(cast(city as varchar)) as city,
        trim(cast(state as varchar)) as state,
        trim(cast(zip_code as varchar)) as zip_code,
        trim(cast(region as varchar)) as region_code,
        case trim(cast(region as varchar))
            when 'NE' then 'Northeast'
            when 'SE' then 'Southeast'
            when 'MW' then 'Midwest'
            when 'SW' then 'Southwest'
            when 'WE' then 'West'
            else 'Unknown'
        end as region_name,
        trim(cast(industry_code as varchar)) as industry_code,
        trim(cast(segment as varchar)) as segment_code,
        case trim(cast(segment as varchar))
            when 'E' then 'Enterprise'
            when 'M' then 'Mid-Market'
            when 'S' then 'Small Business'
            when 'T' then 'Startup'
            else 'Unknown'
        end as segment_name,
        coalesce(credit_limit, 0) as credit_limit,
        coalesce(credit_used, 0) as credit_used,
        coalesce(credit_limit, 0) - coalesce(credit_used, 0) as credit_available,
        payment_terms as payment_terms_days,
        trim(cast(credit_status as varchar)) as credit_status_code,
        case trim(cast(credit_status as varchar))
            when 'A' then 'Active'
            when 'H' then 'Hold'
            when 'S' then 'Suspended'
            else 'Unknown'
        end as credit_status,
        trim(cast(account_status as varchar)) as account_status_code,
        case trim(cast(account_status as varchar))
            when 'A' then 'Active'
            when 'I' then 'Inactive'
            when 'C' then 'Closed'
            else 'Unknown'
        end as account_status,
        case when trim(cast(account_status as varchar)) = 'A' then true else false end as is_active,
        case when trim(cast(credit_status as varchar)) = 'A' then true else false end as is_credit_active,
        created_date,
        updated_date,
        trim(cast(updated_by as varchar)) as updated_by
    from source
    where customer_id is not null
)

select * from cleaned