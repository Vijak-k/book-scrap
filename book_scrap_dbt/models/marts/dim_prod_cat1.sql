WITH prod_cat1 AS (
    SELECT * FROM {{ ref('prod_cat_lv1') }}
),
cast_type AS (
    SELECT
        CAST(product_type_id AS INT) AS product_type_id,
        CAST(category_1_code AS INT) AS cat1_code,
        category_1_title AS cat1_title
    FROM prod_cat1
)
SELECT * FROM cast_type;
