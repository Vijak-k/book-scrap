WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_product_details') }}
),
renamed_and_cleaned AS (
    SELECT
        product_id,
        title,
        CAST(CASE
          WHEN Price_Full = 'N/A' THEN '0'
          ELSE Price_Full END AS INT
        ) AS price,
        NULLIF(barcode, 'N/A') AS barcode,
        CASE WHEN Release_Date = 'N/A' THEN NULL ELSE CAST(Release_Date AS DATE) END AS release_date,
        NULLIF(keywords, 'N/A') AS keywords,
        NULLIF(author, 'N/A') AS author,
        NULLIF(publisher, 'N/A') AS publisher,
        NULLIF(category_lv1, 'N/A') AS category_lv1,
        NULLIF(category_lv2, 'N/A') AS category_lv2,
        AverageRating AS average_rating,
        TotalRating AS rating_count,
        NumberOfPage AS number_of_page,
        Width AS width_cm,
        Height AS height_cm,
        Thick AS thickness_cm,
        GrossWeightKG AS weight_kg,
        FileSizeMB AS file_size_mb,
        scraped_at AS scrape_at,
        url
    FROM source
    WHERE
      status_code = 200 AND
      title != 'N/A'
)
SELECT * FROM renamed_and_cleaned