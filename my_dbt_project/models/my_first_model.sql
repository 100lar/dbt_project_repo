{{ config(
    materialized = 'table'
) }}

with source as (

    select
        customer_id,
        registration_date
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
--order by registration_month
