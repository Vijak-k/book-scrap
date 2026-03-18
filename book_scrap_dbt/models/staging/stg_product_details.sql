WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_product_details') }}
),
clean AS (
  SELECT
    product_id,
    CASE WHEN title = 'N/A' THEN NULL ELSE title END AS title,
    CASE
      WHEN (title = 'N/A' AND Price_Full = 'N/A') THEN NULL
      WHEN (title != 'N/A' AND Price_Full = 'N/A') THEN 0
      ELSE CAST(Price_Full AS INT) END AS price,
    CASE WHEN barcode = 'N/A' THEN NULL ELSE barcode END AS barcode,
    CASE WHEN Release_Date = 'N/A' THEN NULL ELSE CAST(Release_Date AS DATE) END AS release_date,
    CASE WHEN keywords = 'N/A' THEN NULL ELSE keywords END AS keywords, -- เก็บไว้เพื่อตรวจสอบ
    CASE WHEN author = 'N/A' THEN NULL ELSE author END AS author,
    CASE WHEN publisher = 'N/A' THEN NULL ELSE publisher END AS publisher,
    CASE WHEN category_lv1 = 'N/A' THEN NULL ELSE category_lv1 END AS category_lv1,
    CASE WHEN category_lv2 = 'N/A' THEN NULL ELSE category_lv2 END AS category_lv2,
    AverageRating AS average_rating,
    TotalRating AS rating_count,
    NumberOfPage AS number_of_page,
    Width AS width_cm,
    Height AS height_cm,
    Thick AS thickness_cm,
    GrossWeightKG AS weight_kg,
    FileSizeMB AS file_size_mb,
    scraped_at AS scrape_at,
    url AS url
  FROM source
  WHERE Price_Full = 'N/A'
)
SELECT * FROM clean