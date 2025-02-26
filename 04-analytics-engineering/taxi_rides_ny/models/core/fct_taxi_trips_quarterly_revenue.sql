{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue_calc as (
    select
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        service_type,
        sum(total_amount) as total_quarterly_revenue
    from {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
    group by 1, 2, 3
),
quarterly_growth AS (
    SELECT 
        year,
        quarter,
        service_type,
        total_quarterly_revenue,
        LAG(total_quarterly_revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_revenue,
    FROM quarterly_revenue_calc
)
SELECT 
      service_type,
      year,
      quarter,
      total_quarterly_revenue, 
      prev_year_revenue,
      100* ((total_quarterly_revenue - prev_year_revenue)/NULLIF(prev_year_revenue, 0)) as yoy_growth,
FROM quarterly_growth