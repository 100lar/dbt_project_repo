{{ config(
    materialized = 'table',
    schema = 'staging',
    alias = 'registrations'
) }}

with source as (

    select
        customer_id,
        email,
        signup_date as registration_date
    from {{ source('raw', 'customers') }}

),

monthly as (

    select
        format(registration_date, 'yyyy-MM') as registration_month,
        count(distinct email) as customer_count
    from source
    group by format(registration_date, 'yyyy-MM')

)

select *
from monthly
