SELECT
    CAST(product_type_id AS INT) AS product_type_id,
    product_type_description
FROM {{ ref('product_type') }}