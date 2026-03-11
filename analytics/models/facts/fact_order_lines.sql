{{ config(
    schema='FACTS',
) }}

SELECT
	dc.customer_sk
	, dp.product_sk
	, ds.seller_sk
	, ol.order_line_id
	, ol.order_id
    , pp.stock_quantity
	, ol.quantity AS purchase_quantity
    , pp.current_price
	, ol.unit_price_at_sale
	, sum(ol.quantity*ol.unit_price_at_sale) OVER (PARTITION BY ol.order_id) AS line_subtotal
	, o.total_order_amount
	, o.order_date
	, o.status
FROM 
	{{ source('marketplace','order_line') }} ol 
	JOIN {{ source('marketplace','orders') }} o ON ol.order_id = o.order_id
	LEFT JOIN {{ ref('dim_customer') }} dc ON dc.customer_id = o.customer_id
	LEFT JOIN {{ ref('dim_product') }} dp ON dp.product_id = ol.product_id
	LEFT JOIN {{ ref('dim_seller') }} ds ON ds.seller_id = ol.seller_id
    LEFT JOIN {{ source('marketplace','seller_product_price') }}  pp ON pp.seller_id = ol.seller_id AND pp.product_id = ol.product_id