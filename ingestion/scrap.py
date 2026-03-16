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
import os

# 1. หาตำแหน่งของไฟล์ .py ที่กำลังรันอยู่
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. ถอยหลัง 1 step ไปที่ book-scrap แล้วเข้าไปที่ data/
DB_NAME = os.path.join(current_dir, "..", "data", "naiin_inventory.duckdb")

try:
    conn = duckdb.connect(DB_NAME)
    print(f"✅ เชื่อมต่อสำเร็จ: {DB_NAME}")
    conn.close()
except Exception as e:
    print(f"❌ เชื่อมต่อไม่สำเร็จ: {e}")

# Load products urls
csv_name = os.path.join(current_dir, "..", "data", "prod_urls.csv")
prod_url = pd.read_csv(csv_name)
prod_url = prod_url.drop(columns=['status', 'discovered_at'])
prod_url['product_id'] = prod_url['product_id'].astype(int)

def extract_extra_info(html_content):
    extra_data = {}
    # ใช้ BeautifulSoup ช่วยสำหรับส่วนที่ JSON ไม่มีข้อมูล
    soup = BeautifulSoup(html_content, 'lxml')
    
    try:
        # 1. ดึง Rating และข้อมูลเบื้องต้นจาก JSON (วิธีเดิมที่คุณใช้แล้วได้ผล)
        match = re.search(r'AverageRating&quot;:(.+?)\}', html_content)
        if match:
            raw_json = '{"AverageRating":' + match.group(1) + '}'
            clean_json = html.unescape(raw_json)
            json_obj = json.loads(clean_json)
            
            extra_data = {
                'AverageRating': json_obj.get('AverageRating') or None,
                'TotalRating': json_obj.get('TotalRating') or None,
                'NumberOfPage': json_obj.get('NumberOfPage') or None,
                # เริ่มต้นด้วย None เดี๋ยวเราใช้ BeautifulSoup ทับถ้าหาเจอ
                'Width': None,
                'Height': None,
                'Thick': None,
                'GrossWeightKG': None,
                'FileSizeMB': None
            }

        # 2. ใช้ BeautifulSoup เจาะหา "ขนาดไฟล์", "ขนาด", "น้ำหนัก" จาก Label โดยตรง
        # เราจะหา <label> ที่มีข้อความที่ต้องการ แล้วหา <p> ที่อยู่ถัดจากมัน
        labels = soup.find_all('label', class_='product-label')
        for label in labels:
            text = label.get_text(strip=True)
            detail_p = label.find_next_sibling('p', class_='product-label-detail')
            
            if detail_p:
                val = detail_p.get_text(strip=True)
                
                if "ขนาดไฟล์" in text:
                    # ดึงเฉพาะตัวเลขจาก "4.08 MB"
                    size_val = re.search(r'[\d\.]+', val)
                    if size_val: extra_data['FileSizeMB'] = size_val.group()
                
                elif "ขนาด" in text:
                    # แยก "0 x 0 x 0 CM" ออกเป็น 3 ส่วน
                    dims = re.findall(r'[\d\.]+', val)
                    if len(dims) >= 3:
                        extra_data['Width'] = dims[0]
                        extra_data['Height'] = dims[1]
                        extra_data['Thick'] = dims[2]
                
                elif "น้ำหนัก" in text:
                    # ดึงเฉพาะตัวเลขจาก "0.374 KG"
                    weight_val = re.search(r'[\d\.]+', val)
                    if weight_val: extra_data['GrossWeightKG'] = weight_val.group()

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
        'Barcode': soup.find('meta', property='book:isbn'),
        'Release_Date': soup.find('meta', property='book:release_date'),
        'keywords': soup.find('meta', attrs={'name': 'keywords'}),
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
        CREATE TABLE IF NOT EXISTS raw_product_details (
            product_id INTEGER PRIMARY KEY,
            url VARCHAR,
            Title VARCHAR,
            Price_Full VARCHAR,
            Barcode VARCHAR,
            Release_Date VARCHAR,
            keywords VARCHAR,
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
        finished_ids = conn.execute("""
                SELECT product_id 
                FROM raw_product_details 
                WHERE title IS NOT NULL AND title != 'N/A'
            """).df()['product_id'].tolist()
    except:
        finished_ids = []
    
    finished_ids = set(finished_ids)
    targets = df_examples[~df_examples['product_id'].isin(finished_ids)]
    print(f"📦 Total: {len(df_examples)} | Finished: {len(finished_ids)} | Remaining: {len(targets)}")

    batch_data = []
    cols = ['product_id', 'url', 'Title', 'Price_Full', 
            'Barcode', 'Release_Date', 'keywords',
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
                    conn.execute("INSERT INTO raw_product_details SELECT * FROM batch_df ON CONFLICT DO NOTHING")
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
            conn.execute("INSERT INTO raw_product_details SELECT * FROM batch_df ON CONFLICT DO NOTHING")
        
        conn.close()
        print("\n✅ Pipeline closed safely.")

if __name__ == "__main__":
    run_main_pipeline(prod_url)