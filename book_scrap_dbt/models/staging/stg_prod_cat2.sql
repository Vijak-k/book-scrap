SELECT
    CAST(product_type_id AS INT) AS product_type_id,
    CAST(category_1_code AS INT) AS category_1_code,
    CAST(category_2_code AS INT) AS category_2_code,
    category_2_title,
    CAST(status_code AS INT) AS status_code,
    url
FROM {{ ref('prod_cat_lv2') }}