{{ config(
    schema='REPORTING',
) }}

with get_unique_orders as (
	select
		*
		, row_number() over(partition by ol.product_sk,order_id) as rn
	from 
		{{ ref('fact_order_lines') }} ol join {{ ref('dim_product') }} ds
		ON ol.product_sk = ds.product_sk
	where
		status<>'cancelled'
)
select
	product_name
	, sum(total_order_amount) as total_order_amount
from
	get_unique_orders
where
	rn=1
group by product_name
order by total_order_amount desc