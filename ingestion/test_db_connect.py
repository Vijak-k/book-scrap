import os
import duckdb

current_dir = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(current_dir, "..", "data", "naiin_inventory.duckdb")

def run_test():
    print(f"Current directory: {current_dir}")
    try:
        conn = duckdb.connect(DB_NAME)
        print(f"✅ เชื่อมต่อสำเร็จ: {DB_NAME}")
        
        # ใช้ SQL มาตรฐานแทน .tables
        tables = conn.execute("SHOW TABLES").fetchall()
        
        if tables:
            print(f"Tables found: {tables}")
            # ลองนับจำนวนข้อมูลในตารางที่คุณเปลี่ยนชื่อดูครับ
            count = conn.execute("SELECT count(*) FROM raw_product_details").fetchone()[0]
            print(f"📊 จำนวนข้อมูลใน raw_product_details: {count:,} แถว")
        else:
            print("⚠️ ไม่พบตารางในฐานข้อมูล (Database is empty)")
            
        conn.close()
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")

if __name__ == "__main__":
    run_test()