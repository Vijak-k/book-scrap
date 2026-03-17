WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_product_details') }}
),
clean AS (
  SELECT
    product_id,
    title,
    CASE WHEN Price_Full = 'N/A' THEN 0
      ELSE CAST(Price_Full AS INT) END AS price,
    Barcode AS barcode,
    CASE WHEN Release_Date = 'N/A' THEN NULL
      ELSE CAST(Release_Date AS DATE) END AS release_date,
    keywords, -- เก็บไว้เพื่อตรวจสอบ
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
  WHERE status_code = 200
),
split_keywords AS (
  SELECT 
    *,
    -- แยกข้อความด้วย comma และตัดช่องว่างส่วนเกินออก (trim)
    string_split(keywords, ',') as kw_list
  FROM clean
)
SELECT
  product_id,
  title,
  -- ดึงตามลำดับที่คุณระบุ (Index 1-5 และที่เหลือคือ Tags)
  trim(kw_list[1]) as kw_title,
  trim(kw_list[2]) as kw_author,
  trim(kw_list[3]) as kw_publisher,
  trim(kw_list[4]) as kw_cat_lv1,
  trim(kw_list[5]) as kw_cat_lv2,
  trim(kw_list[6]) as tag1,
  trim(kw_list[7]) as tag2,
  trim(kw_list[8]) as tag3,
  trim(kw_list[9]) as tag4,
  trim(kw_list[10]) as tag5,
  trim(kw_list[11]) as tag6,
  trim(kw_list[12]) as tag7,
  trim(kw_list[13]) as tag8,
  trim(kw_list[14]) as tag9,
  trim(kw_list[15]) as tag10,
  price,
  barcode,
  release_date,
  average_rating,
  rating_count,
  number_of_page,
  scrape_at,
  url
FROM split_keywords
WHERE trim(kw_list[15]) IS NOT NULL;