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
DB_NAME = os.path.join(current_dir, "..", "data", "naiin_products.duckdb")

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

def scrape(url, session):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.naiin.com/',
    }
    
    # เตรียมข้อมูลตั้งต้น
    data = {
        'url': url,
        'status_code': None,
        'Title': "N/A",
        'Price_Full': "N/A",
        'Barcode': "N/A",
        'Release_Date': "N/A",
        'keywords': "N/A",
        'product_type': "N/A",
        'author': "N/A",
        'publisher': "N/A",
        'category_lv1': "N/A",
        'category_lv2': "N/A"
    }
    
    try:
        time.sleep(random.uniform(1.2,1.6))
        response = session.get(url, headers=headers, timeout=15)
        data['status_code'] = response.status_code
        
        # ถ้า status ไม่ใช่ 200 (เช่น 404) ให้คืนค่าเลย ไม่ต้องเสียเวลา parse
        if response.status_code != 200:
            return data

        html_text = response.text
        soup = BeautifulSoup(html_text, 'lxml')
        
        # 1. ดึง Metadata ปกติ
        meta_map = {
            'Title': soup.find('meta', property='og:title'),
            'Price_Full': soup.find('meta', property='og:product:price:amount'),
            'Barcode': soup.find('meta', property='book:isbn'),
            'Release_Date': soup.find('meta', property='book:release_date'),
        }
        
        for key, tag in meta_map.items():
            if tag and tag.has_attr('content'):
                data[key] = tag['content'].strip()

        # 2. ดึง Keywords ด้วย Greedy Regex
        kw_pattern = r'<meta[^>]*name="keywords"[^>]*content="(.*)"\s*/?>'
        kw_match = re.search(kw_pattern, html_text, re.IGNORECASE | re.DOTALL)

        if kw_match:
            raw_content = kw_match.group(1).strip()
            clean_content = raw_content.split('">')[0]
            data['keywords'] = clean_content.rstrip('"')

        # 3. ดึง Extra Info (Table/Rating)
        extra_info = extract_extra_info(html_text)
        data.update(extra_info)

        # 4. ดึง Product Type จาก Breadcrumbs ---
        data['product_type'] = "N/A" # ค่าตั้งต้น
        breadcrumb_div = soup.find('div', class_='breadcrumbs')

        if breadcrumb_div:
            # หา <a> ทั้งหมดใน breadcrumbs
            links = breadcrumb_div.find_all('a')
            # ปกติ links[0] คือ "หน้าแรก", links[1] คือ "Product Type"
            if len(links) >= 2:
                data['product_type'] = links[1].get_text(strip=True)
        
        # 5. ดึงข้อมูล ผู้เขียน, สำนักพิมพ์, หมวดหมู่ (ส่วนที่เพิ่มใหม่)
        container = soup.find('div', class_='bookdetail-container')
        if container:
            labels = container.find_all('label', class_='product-label')
            for label in labels:
                text = label.get_text(strip=True)
                # หาแท็ก <a> ที่อยู่ถัดจาก label
                value_tag = label.find_next_sibling('a', class_='link-book-detail')
                
                if value_tag:
                    value = value_tag.get_text(strip=True)
                    if "ผู้เขียน:" in text:
                        data['author'] = value
                    elif "สำนักพิมพ์:" in text:
                        data['publisher'] = value
                    elif "หมวดหมู่:" in text:
                        data['category_lv1'] = value
                        # หาหมวดหมู่ย่อย (ถ้ามี) ซึ่งจะเป็น <a> ตัวถัดไป
                        lv2_tag = value_tag.find_next_sibling('a', class_='link-book-detail')
                        if lv2_tag:
                            data['category_lv2'] = lv2_tag.get_text(strip=True)

    except Exception as e:
        print(f"  [Request Error] {url}: {e}")
        data['status_code'] = 999 # กำหนดรหัสพิเศษสำหรับ Network Error
        data['Title'] = None
        
    return data

def run_main_pipeline(df_examples):
    conn = duckdb.connect(DB_NAME)
    
    # 1. สร้าง Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_product_details (
            product_id INTEGER PRIMARY KEY,
            url VARCHAR,
            status_code INTEGER,
            Title VARCHAR,
            Price_Full VARCHAR,
            Barcode VARCHAR,
            Release_Date VARCHAR,
            keywords VARCHAR,
            product_type VARCHAR,
            author VARCHAR,
            publisher VARCHAR,
            category_lv1 VARCHAR,
            category_lv2 VARCHAR,
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
                WHERE title IS NOT NULL OR status_code = 404
            """).df()['product_id'].tolist()
    except:
        finished_ids = []
    
    finished_ids = set(finished_ids)
    targets = df_examples[~df_examples['product_id'].isin(finished_ids)]
    print(f"📦 Total: {len(df_examples)} | Finished: {len(finished_ids)} | Remaining: {len(targets)}")

    batch_data = []
    cols = [
            'product_id', 'url', 'status_code', 'Title', 'Price_Full', 
            'Barcode', 'Release_Date', 'keywords',
            'product_type', 'author', 'publisher', 'category_lv1', 'category_lv2',
            'AverageRating', 'TotalRating', 'NumberOfPage', 
            'Width', 'Height', 'Thick', 'GrossWeightKG', 
            'FileSizeMB', 'scraped_at'
        ]

    # 3. Main Loop
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount('https://', adapter)
    try:
        for _, row in tqdm(targets.iterrows(), total=len(targets), desc="Scraping"):
            p_id = row['product_id']
            url = row['url']
            
            try:
                res = scrape(url, session) 
                final_row = {
                    'product_id': p_id,
                    **res,
                    'scraped_at': datetime.now()
                }
                batch_data.append(final_row)
                
                # 4. Batch Commit
                if len(batch_data) >= 20:
                    batch_df = pd.DataFrame(batch_data)
                    batch_df = batch_df.reindex(columns=cols)
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
            for col in cols:
                if col not in batch_df.columns:
                    batch_df[col] = None
            batch_df = batch_df[cols]
            conn.execute("INSERT INTO raw_product_details SELECT * FROM batch_df ON CONFLICT DO NOTHING")
        
        conn.close()
        print("\n✅ Pipeline closed safely.")

if __name__ == "__main__":
    run_main_pipeline(prod_url)