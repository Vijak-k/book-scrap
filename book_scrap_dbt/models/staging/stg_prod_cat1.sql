WITH prod_cat1 AS (
    SELECT * FROM {{ ref('prod_cat_lv1') }}
)
SELECT * FROM prod_cat1;