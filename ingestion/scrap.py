import pandas as pd
import duckdb
import time
import random
from datetime import datetime
from tqdm import tqdm
import re
import json
import html
import requests
from bs4 import BeautifulSoup

DB_NAME = "naiin_inventory.duckdb"

# โหลดข้อมูลและจัดการ Type ให้ตรงกับ Schema (ใช้ str สำหรับ ID)
prod_url = pd.read_csv('prod_urls.csv')
prod_url = prod_url.drop(columns=['status', 'discovered_at'])
prod_url['product_id'] = prod_url['product_id'].astype(int)

def extract_extra_info(html_content):
    extra_data = {}
    try:
        match = re.search(r'AverageRating&quot;:(.+?)\}', html_content)
        if match:
            raw_json = '{"AverageRating":' + match.group(1) + '}'
            clean_json = html.unescape(raw_json)
            json_obj = json.loads(clean_json)
            
            # เรียง Key ให้ตรงกับลำดับใน Schema ของ DuckDB
            extra_data = {
                'AverageRating': json_obj.get('AverageRating') or None,
                'TotalRating': json_obj.get('TotalRating') or None,
                'NumberOfPage': json_obj.get('NumberOfPage') or None,
                'Width': json_obj.get('Width') or None,
                'Height': json_obj.get('Height') or None,
                'Thick': json_obj.get('Thick') or None,
                'GrossWeightKG': json_obj.get('GrossWeight') or None,
                'FileSizeMB': json_obj.get('FileSize') or None 
            }
    except Exception as e:
        print(f"  [Warning] Extra info skip: {e}")
    return extra_data

def scrape(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.naiin.com/',
    }

    time.sleep(random.uniform(0.8, 1.2))
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'lxml')
    
    data = {'url': url}
    meta_map = {
        'Title': soup.find('meta', property='og:title'),
        'Price_Full': soup.find('meta', property='og:product:price:amount'),
        'Price_Sale': soup.find('meta', property='og:product:sale_price:amount'),
        'Barcode': soup.find('meta', property='book:isbn'),
        'Release_Date': soup.find('meta', property='book:release_date'),
        'keywords': soup.find('meta', attrs={'name': 'keywords'}),
        'Description': soup.find('meta', property='og:description'),
    }

    for key, tag in meta_map.items():
        data[key] = tag['content'] if tag else "N/A"

    extra_info = extract_extra_info(response.text)
    data.update(extra_info)
    return data

def run_main_pipeline(df_examples):
    conn = duckdb.connect(DB_NAME)
    
    # 1. สร้าง Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_details (
            product_id INTEGER PRIMARY KEY,
            url VARCHAR,
            Title VARCHAR,
            Price_Full VARCHAR,
            Price_Sale VARCHAR,
            Barcode VARCHAR,
            Release_Date VARCHAR,
            keywords VARCHAR,
            Description VARCHAR,
            AverageRating DOUBLE,
            TotalRating INTEGER,
            NumberOfPage INTEGER,
            Width DOUBLE,
            Height DOUBLE,
            Thick DOUBLE,
            GrossWeightKG DOUBLE,
            FileSizeMB DOUBLE,
            scraped_at TIMESTAMP
        )
    """)

    # 2. Checkpoint
    try:
        finished_ids = conn.execute("SELECT product_id FROM product_details").df()['product_id'].tolist()
    except:
        finished_ids = []
    
    finished_ids = set(finished_ids)
    targets = df_examples[~df_examples['product_id'].isin(finished_ids)]
    print(f"📦 Total: {len(df_examples)} | Finished: {len(finished_ids)} | Remaining: {len(targets)}")

    batch_data = []
    cols = ['product_id', 'url', 'Title', 'Price_Full', 'Price_Sale', 
            'Barcode', 'Release_Date', 'keywords', 'Description', 
            'AverageRating', 'TotalRating', 'NumberOfPage', 
            'Width', 'Height', 'Thick', 'GrossWeightKG', 
            'FileSizeMB', 'scraped_at']

    # 3. Main Loop
    try:
        for _, row in tqdm(targets.iterrows(), total=len(targets), desc="Scraping"):
            p_id = row['product_id']
            url = row['url']
            
            try:
                res = scrape(url) 
                final_row = {
                    'product_id': p_id,
                    **res,
                    'scraped_at': datetime.now()
                }
                batch_data.append(final_row)
                
                # 4. Batch Commit (ย้ายเข้ามาอยู่ใน Loop แล้ว)
                if len(batch_data) >= 20:
                    batch_df = pd.DataFrame(batch_data)
                    batch_df = batch_df[cols] # บังคับลำดับคอลัมน์
                    conn.execute("INSERT INTO product_details SELECT * FROM batch_df ON CONFLICT DO NOTHING")
                    batch_data = [] 
                    
            except Exception as e:
                print(f"\n❌ Error ID {p_id}: {e}")
                time.sleep(5)
                continue

    except KeyboardInterrupt:
        print("\n🛑 Progress saved. Stopping safely...")
    
    finally:
        # เก็บตก Batch สุดท้าย
        if batch_data:
            batch_df = pd.DataFrame(batch_data)
            batch_df = batch_df[cols]
            conn.execute("INSERT INTO product_details SELECT * FROM batch_df ON CONFLICT DO NOTHING")
        
        conn.close()
        print("\n✅ Pipeline closed safely.")

if __name__ == "__main__":
    run_main_pipeline(prod_url)