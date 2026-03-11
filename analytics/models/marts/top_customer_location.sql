{{ config(
    schema='REPORTING',
) }}

with get_unique_orders as (
	select
		ol.customer_sk
		, full_address
		, total_order_amount
		, first_name  || ' ' || last_name as full_name
		, row_number() over(partition by ol.customer_sk,order_id) as rn
	from 
		{{ ref('fact_order_lines') }} ol join {{ ref('dim_customer') }} ds
		ON ol.customer_sk = ds.customer_sk
	where
		status<>'cancelled'
)
select
	 max(full_name)  as full_name
	, max(full_address) as full_address
	, sum(total_order_amount) as total_order_amount
from
	get_unique_orders
where
	rn=1
group by customer_sk
order by total_order_amount desc