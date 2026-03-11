{{ config(
    schema='DIMENSIONS',
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} AS seller_sk
    , seller_id
    , store_name 
    , rating
    , joined_date
    , CASE
        WHEN rating >= 4.5 THEN 'Top Rated'
        WHEN rating >= 4.0 THEN 'High Rated'
        WHEN rating >= 3.0 THEN 'Average'
        ELSE 'Needs Improvement'
    END AS seller_tier
FROM {{ source('marketplace','seller') }}