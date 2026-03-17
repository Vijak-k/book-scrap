WITH prod_cat2 AS (
    SELECT * FROM {{ ref('prod_cat_lv2') }}
)
SELECT * FROM prod_cat2;