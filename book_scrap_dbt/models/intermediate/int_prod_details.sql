WITH staging AS (
    SELECT * FROM {{ ref('stg_prod_details') }}
),
clean_title_1 AS (
    SELECT
        product_id,
        TRIM(split_part(keywords, ',', 1)) AS product_title,
        title,
        REGEXP_REPLACE(
            REGEXP_EXTRACT(title, '^(.*?)(?:\s*\d*)?\s*(?:Books|E-Books|\(Mg\)|\(LN\))', 1),
            '\s*เล่ม[\s\d\-จบ\(\)\.\,]*',
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
),
clean_title_2 AS (
    SELECT
        product_id,
        product_title,
        title,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    -- Step 1: Extract the part before 'Books|E-Books|...'
                    REGEXP_EXTRACT(title, '^(.*?)(?:\s*\d*)?\s*(?:Books|E-Books|\(Mg\)|\(LN\))', 1),
                    -- Step 2: Remove Prefixes (SET, BOOKSET, ชุด) at the start
                    '^(SET|BOOKSET|ชุด)\s*', '', 'i'
                ),
                -- Step 3: THE TRUNCATOR
                -- Remove everything starting from:
                -- A space followed by a number (\s+\d+)
                -- OR a number followed by a hyphen (\d+-)
                -- OR a parenthesis containing 'เล่ม' or 'จบ' (\(.*\))
                -- OR the word 'เล่ม' or 'จบ'
                '(\s+\d+|\d+-|\s*\(.*(เล่ม|จบ).*\)|เล่ม|จบ).*$', 
                '', 
                'g'
            )
        ) AS series_title,
        
        raw_series_number,
        is_bundle,
        is_finished,
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
    FROM clean_title_1
)
SELECT * FROM clean_title_2
WHERE title LIKE 'ชุด%'