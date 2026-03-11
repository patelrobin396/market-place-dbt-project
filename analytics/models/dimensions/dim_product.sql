{{ config(
    schema='DIMENSIONS',
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk
	, product_id
	, product_name
	, category 
	, brand
FROM {{ source('marketplace','product') }}