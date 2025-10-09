#!/usr/bin/env python3
"""
Update Latest MOT Data Script - Simple Version

ดึงข้อมูลล่าสุดจาก MOT API และเติมเข้า PostgreSQL features table
ใช้ query เฉพาะเดือนที่ต้องการ

Current situation:
- DB latest: 2025-09-03 (September)  
- Today: 2025-10-01 (October)
- Query: September + October 2025
"""

import psycopg2
import pandas as pd
import requests
from datetime import datetime, date
import sys
from typing import Optional, Dict, List, Any

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def get_latest_date_from_db() -> Optional[date]:
    """ตรวจสอบวันที่ล่าสุดในฐานข้อมูล"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("SELECT MAX(feature_date) FROM features;")
        result = cur.fetchone()
        latest_date = result[0] if result[0] else None
        
        cur.close()
        conn.close()
        
        return latest_date
    except Exception as e:
        print(f"❌ Error getting latest date: {e}")
        return None

def fetch_mot_data_for_months(year: int, months: List[int]) -> Optional[List[Dict[str, Any]]]:
    """ดึงข้อมูล MOT API สำหรับเดือนที่ระบุ"""
    url = "https://datagov.mot.go.th/api/action/datastore_search"
    all_records = []
    
    for month in months:
        query_month = f"{year}-{month:02d}"
        print(f"🔍 กำลังค้นหาเดือน: {query_month}")
        
        params = {
            "resource_id": "a139ab23-602f-4c0d-8789-4d230bcdf33d",
            "q": f"{query_month}",
            "limit": 10000
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'result' not in data or 'records' not in data['result']:
                print(f"❌ ไม่พบข้อมูลในเดือน {query_month}")
                continue
            
            records = data['result']['records']
            print(f"📊 เดือน {query_month}: ดึงข้อมูลได้ {len(records)} รายการ")
            all_records.extend(records)
        
        except Exception as e:
            print(f"❌ ข้อผิดพลาดเดือน {query_month}: {e}")
            continue
    
    return all_records if all_records else None

def filter_new_records(records: List[Dict[str, Any]], since_date: date) -> List[Dict[str, Any]]:
    """กรองเฉพาะข้อมูลใหม่"""
    new_records = []
    for record in records:
        if 'วันที่' in record and record['วันที่']:
            try:
                record_date = pd.to_datetime(record['วันที่']).date()
                if record_date > since_date:
                    new_records.append(record)
            except:
                continue
    
    print(f"🆕 พบข้อมูลใหม่: {len(new_records)} รายการ")
    return new_records

def process_mot_data(records: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """ประมวลผลข้อมูล MOT เป็น DataFrame สำหรับฐานข้อมูล"""
    if not records:
        print("❌ ไม่มีข้อมูลให้ประมวลผล")
        return None
    
    print("🔄 กำลังประมวลผลข้อมูล...")
    
    df = pd.DataFrame(records)
    # df.to_csv('checkpoint.csv', index=False)
    # แปลงวันที่
    df['วันที่'] = pd.to_datetime(df['วันที่'], errors='coerce')
    df = df.dropna(subset=['วันที่'])
    
    if df.empty:
        print("❌ ไม่มีข้อมูลที่มีวันที่ถูกต้อง")
        return None
    
    # แปลงค่าผู้โดยสาร
    df['ปริมาณ'] = pd.to_numeric(df['ปริมาณ'], errors='coerce').fillna(0)
    
    # สร้าง pivot table
    pivot_df = df.pivot_table(
        index='วันที่',
        columns='ยานพาหนะ/ท่า',
        values='ปริมาณ',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # แสดงคอลัมน์ที่ได้จาก pivot
    pivot_columns = [col for col in pivot_df.columns if col != 'วันที่']
    print(f"📊 คอลัมน์จาก pivot: {pivot_columns}")
    
    # Merge คอลัมน์ที่มีชื่อคล้ายกัน (ป้องกันการเขียนไม่สม่ำเสมอ)
    
    # กลุ่มคำสำคัญสำหรับแต่ละสาย
    line_groups = {
        'ARL': ['arl', 'ARL', 'สุวรรณภูมิ'],
        'BTS': ['bts', 'BTS', 'ลูกฟูก'],
        'น้ำเงิน': ['น้ำเงิน', 'นำเงิน', 'น้าเงิน', 'สีน้ำเงิน'],
        'ม่วง': ['ม่วง', 'มวง', 'สีม่วง'],
        'ชมพู': ['ชมพู', 'ชมพูู', 'ชมพุ', 'สีชมพู'],
        'แดง': ['แดง', 'แดง', 'สีแดง', 'ชานเมือง'],
        'เหลือง': ['เหลือง', 'เหลืง', 'สีเหลือง']
    }
    
    for line_key, keywords in line_groups.items():
        # หาคอลัมน์ที่ตรงกับกลุ่มนี้
        matching_cols = []
        for col in pivot_columns:
            if any(keyword in col for keyword in keywords):
                matching_cols.append(col)
        
        if len(matching_cols) > 1:
            print(f"🔄 พบคอลัมน์ {line_key} หลายตัว: {matching_cols}")
            # รวมค่าทั้งหมดเข้าคอลัมน์แรก
            main_col = matching_cols[0]
            for other_col in matching_cols[1:]:
                pivot_df[main_col] = pivot_df[main_col] + pivot_df[other_col]
                pivot_df = pivot_df.drop(columns=[other_col])
            print(f"✅ รวม {line_key} เป็น: {main_col}")
        elif len(matching_cols) == 1:
            print(f"✅ {line_key}: {matching_cols[0]}")
    
    # อัปเดตรายการคอลัมน์หลังจาก merge
    pivot_columns = [col for col in pivot_df.columns if col != 'วันที่']
    print(f"📊 คอลัมน์หลัง merge: {pivot_columns}")

    # mapping จากชื่อยานพาหนะไป column ภาษาอังกฤษ
    vehicle_mapping = {
        'รถไฟฟ้า ARL': 'arl',
        'รถไฟฟ้า BTS': 'bts', 
        'รถไฟฟ้าสายสีน้ำเงิน': 'mrt_blue',
        'รถไฟฟ้าสายสีม่วง': 'mrt_purple',
        'รถไฟฟ้าสายสีชมพู': 'mrt_pink',
        'รถไฟฟ้าสายสีแดง': 'srt_red',
        'รถไฟฟ้าสายสีเหลือง': 'mrt_yellow'
    }
    
    # เตรียมข้อมูลสำหรับฐานข้อมูล
    result_data = []
    pivot_columns = [col for col in pivot_df.columns if col != 'วันที่']
    
    for _, row in pivot_df.iterrows():
        record_date = row['วันที่'].date()
        dow = record_date.weekday()  # 0=Monday, 6=Sunday
        
        # สร้าง record
        record = {
            'feature_date': record_date,
            'day_type': -1,  # ตั้งค่าเป็น -1 ตามที่ร้องขอ
            'dow': dow,
            'rain_average': None,  # ตั้งค่าเป็น null ตามที่ร้องขอ
            'arl': 0.0,
            'bts': 0.0,
            'mrt_blue': 0.0,
            'mrt_purple': 0.0,
            'srt_red': 0.0,
            'mrt_yellow': 0.0,
            'mrt_pink': 0.0
        }
        
        # เติมข้อมูลยานพาหนะจาก pivot table
        for pivot_col in pivot_columns:
            for vehicle_name, target_col in vehicle_mapping.items():
                if vehicle_name in pivot_col:
                    record[target_col] = float(row.get(pivot_col, 0))
                    break
        
        result_data.append(record)
    
    result_df = pd.DataFrame(result_data)
    
    # เรียงลำดับตามวันที่ก่อน return
    result_df = result_df.sort_values('feature_date').reset_index(drop=True)
    
    print(f"✅ ประมวลผลเสร็จสิ้น: {len(result_df)} รายการ")
    print(f"📅 ช่วงวันที่: {result_df['feature_date'].min()} ถึง {result_df['feature_date'].max()}")
    
    # แสดงตัวอย่างข้อมูลที่ประมวลผล (3 รายการแรก)
    print("\n📊 ตัวอย่างข้อมูลที่ประมวลผล:")
    for idx, row in result_df.head(3).iterrows():
        transport_info = []
        if row['arl'] > 0: transport_info.append(f"ARL={row['arl']:.0f}")
        if row['bts'] > 0: transport_info.append(f"BTS={row['bts']:.0f}")
        if row['mrt_blue'] > 0: transport_info.append(f"Blue={row['mrt_blue']:.0f}")
        if row['mrt_purple'] > 0: transport_info.append(f"Purple={row['mrt_purple']:.0f}")
        if row['mrt_pink'] > 0: transport_info.append(f"Pink={row['mrt_pink']:.0f}")
        if row['srt_red'] > 0: transport_info.append(f"Red={row['srt_red']:.0f}")
        if row['mrt_yellow'] > 0: transport_info.append(f"Yellow={row['mrt_yellow']:.0f}")
        
        transport_str = ", ".join(transport_info) if transport_info else "ไม่มีข้อมูล"
        print(f"  {row['feature_date']}: {transport_str}")
    
    return result_df

def insert_new_data(df: pd.DataFrame) -> bool:
    """บันทึกข้อมูลใหม่เข้าฐานข้อมูล พร้อมเรียงลำดับวันที่ถูกต้อง"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # เรียงลำดับข้อมูลตามวันที่ก่อนบันทึก (เก่าสุดก่อน)
        df = df.sort_values('feature_date').reset_index(drop=True)
        
        # เรียงลำดับ columns ให้ตรงกับตาราง
        cols = [
            "feature_date", "day_type", "dow", "rain_average",
            "arl", "bts", "mrt_blue", "mrt_purple",
            "srt_red", "mrt_yellow", "mrt_pink"
        ]
        df = df[cols]
        
        # แสดงข้อมูลที่จะบันทึก
        print(f"💾 กำลังบันทึกข้อมูล {len(df)} รายการ (เรียงตามวันที่)...")
        print(f"📅 ช่วงวันที่: {df['feature_date'].iloc[0]} ถึง {df['feature_date'].iloc[-1]}")
        
        if len(df) <= 7:
            for idx, row in df.iterrows():
                transport_info = []
                if row['arl'] > 0: transport_info.append(f"ARL={row['arl']:.0f}")
                if row['bts'] > 0: transport_info.append(f"BTS={row['bts']:.0f}")
                transport_str = ", ".join(transport_info) if transport_info else "ไม่มีข้อมูล"
                print(f"  {row['feature_date']}: {transport_str}")
        else:
            print(f"  ตัวอย่างแรก: {df.iloc[0]['feature_date']}")
            print(f"  ตัวอย่างสุดท้าย: {df.iloc[-1]['feature_date']}")
        
        # ใช้ bulk UPSERT เพื่อประสิทธิภาพที่ดีกว่า
        upsert_sql = """
        INSERT INTO features (feature_date, day_type, dow, rain_average, arl, bts, mrt_blue, mrt_purple, srt_red, mrt_yellow, mrt_pink)
        VALUES %s
        ON CONFLICT (feature_date) 
        DO UPDATE SET 
            day_type = EXCLUDED.day_type,
            dow = EXCLUDED.dow,
            rain_average = EXCLUDED.rain_average,
            arl = EXCLUDED.arl,
            bts = EXCLUDED.bts,
            mrt_blue = EXCLUDED.mrt_blue,
            mrt_purple = EXCLUDED.mrt_purple,
            srt_red = EXCLUDED.srt_red,
            mrt_yellow = EXCLUDED.mrt_yellow,
            mrt_pink = EXCLUDED.mrt_pink
        """
        
        # เตรียมข้อมูลสำหรับ bulk insert
        from psycopg2.extras import execute_values
        values = [tuple(row) for row in df.values]
        
        # Execute bulk insert
        execute_values(cur, upsert_sql, values, template=None, page_size=100)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ บันทึกข้อมูลสำเร็จ: {len(df)} รายการ (เรียงตามวันที่)")
        return True
        
    except Exception as e:
        print(f"❌ ข้อผิดพลาดในการบันทึก: {e}")
        return False

def main():
    """ฟังก์ชันหลัก"""
    print("🚀 เริ่มต้นการอัปเดตข้อมูล MOT")
    
    # 1. ตรวจสอบวันที่ล่าสุดในฐานข้อมูล
    latest_date = get_latest_date_from_db()
    if latest_date is None:
        print("❌ ไม่สามารถตรวจสอบวันที่ล่าสุดได้")
        return False
    
    print(f"📅 วันที่ล่าสุดในฐานข้อมูล: {latest_date}")
    
    # 2. กำหนดเดือนที่ต้อง query อัตโนมัติ
    from datetime import datetime
    
    # ข้อมูลจากฐานข้อมูล
    latest_year = latest_date.year
    latest_month = latest_date.month
    
    # วันที่ปัจจุบัน
    today = datetime.now().date()
    current_year = today.year
    current_month = today.month
    
    print(f"📊 เปรียบเทียบ: DB({latest_year}-{latest_month:02d}) vs ปัจจุบัน({current_year}-{current_month:02d})")
    
    # กำหนดเดือนที่ต้อง query
    months_to_query = []
    years_to_query = set()
    
    if latest_year == current_year and latest_month == current_month:
        # เดือนเดียวกัน - query เฉพาะเดือนปัจจุบัน
        months_to_query = [(current_year, current_month)]
        print(f"✅ เดือนเดียวกัน: query เฉพาะ {current_year}-{current_month:02d}")
    else:
        # ต่างเดือน - query ทั้งเดือนล่าสุดและปัจจุบัน
        months_to_query = [(latest_year, latest_month), (current_year, current_month)]
        
        # ลบ duplicate ถ้ามี
        months_to_query = list(set(months_to_query))
        months_to_query.sort()  # เรียงลำดับ
        
        print(f"� ต่างเดือน: query {len(months_to_query)} เดือน")
        for year, month in months_to_query:
            print(f"   - {year}-{month:02d}")
    
    # แยก query ตามปี เพื่อให้ API ทำงานได้ดี
    query_plan = {}
    for year, month in months_to_query:
        if year not in query_plan:
            query_plan[year] = []
        query_plan[year].append(month)
    
    print(f"🗓️  แผนการค้นหา: {dict(query_plan)}")
    
    # 3. ดึงข้อมูลจาก API ตาม query plan
    all_records = []
    for year, months in query_plan.items():
        print(f"\n🔍 กำลังดึงข้อมูลปี {year}")
        year_records = fetch_mot_data_for_months(year, months)
        if year_records:
            all_records.extend(year_records)
    
    if not all_records:
        print("❌ ไม่สามารถดึงข้อมูลจาก API ได้")
        return False
    
    print(f"📈 รวมข้อมูลทั้งหมด: {len(all_records)} รายการ")
    
    # 4. กรองเฉพาะข้อมูลใหม่
    new_records = filter_new_records(all_records, latest_date)
    if not new_records:
        print("✅ ไม่มีข้อมูลใหม่ที่ต้องอัปเดต")
        return True
    
    # 5. ประมวลผลข้อมูล
    df = process_mot_data(new_records)
    if df is None or df.empty:
        print("❌ ไม่สามารถประมวลผลข้อมูลได้")
        return False
    
    # 6. บันทึกเข้าฐานข้อมูล
    success = insert_new_data(df)
    
    if success:
        print("🎉 การอัปเดตข้อมูลเสร็จสิ้นสำเร็จ!")
        return True
    else:
        print("❌ การอัปเดตข้อมูลล้มเหลว")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)