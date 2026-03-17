WITH prod_type AS (
    SELECT * FROM {{ ref('product_type') }}
)
SELECT * FROM prod_type;