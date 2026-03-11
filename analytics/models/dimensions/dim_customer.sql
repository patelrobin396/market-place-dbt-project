{{ config(
    schema='DIMENSIONS',
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'cd.address_id', 'cd.state','cd.zip_code']) }} as customer_sk
	, c.customer_id
	, c.first_name
	, c.last_name
	, c.email
	, cd.address_line_1 as address_line
	, cd.city
	, cd.state
	, cd.zip_code
	, concat(cd.address_line_1, ', ', cd.city, ', ', cd.state, ' ', cd.zip_code) as full_address
    , c.created_at
	, cd.is_default_shipping
FROM 
	{{ source('marketplace','customer') }} c 
    LEFT JOIN {{ source('marketplace','customer_address') }} cd 
    ON c.customer_id = cd.customer_id