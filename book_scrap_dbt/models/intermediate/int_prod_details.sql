WITH staging AS (
    SELECT * FROM {{ ref('stg_prod_details') }}
),
extracted AS (
    SELECT
        product_id,
        TRIM(split_part(keywords, ',', 1)) AS first_keyword,
        title,
        REGEXP_REPLACE(
            REGEXP_EXTRACT(title, '^(.*?)(?:\s*\d*)?\s*(?:Books|E-Books|\(Mg\)|\(LN\))', 1),
            '\s*เล่ม\s*', 
            '', 
            'g'
        ) AS extracted_series_name,
        REGEXP_EXTRACT(title, '(\d+)(?:Books|E-Books|\(Mg\)|\(LN\))', 1) AS raw_series_number,
        CASE 
            WHEN title LIKE 'SET%' OR title LIKE 'BOOK SET%' OR title LIKE 'ชุด %' THEN TRUE 
            ELSE FALSE 
        END AS is_bundle,
        CASE
            WHEN title LIKE '%จบ%' THEN TRUE
            ELSE FALSE
        END AS is_finished,
        price,
        barcode,
        release_date,
        keywords,
        author,
        publisher,
        category_lv1,
        category_lv2,
        average_rating,
        rating_count,
        number_of_page,
        width_cm,
        height_cm,
        thickness_cm,
        weight_kg,
        file_size_mb,
        scrape_at,
        url
    FROM staging
)
SELECT * FROM extracted WHERE title LIKE 'ชุด %'