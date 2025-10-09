#!/usr/bin/env python3
"""
Day Type Collection Spark Application - Functional Version

ใช้งานร่วมกับ Airflow DAG สำหรับดึงข้อมูลวันหยุดจาก myhora.com
และอัพเดต day_type เข้า PostgreSQL features table

Based on: day_type_collecting_functional.py (working version)
Adapted for: Spark + Airflow environment
Purpose: Update day_type column for newly added MOT data (where day_type = -1)
"""

import psycopg2
import pandas as pd
import requests
import os
import time
from datetime import datetime, date, timedelta
import calendar
import sys
from typing import Optional, Dict, List, Any, Tuple

# Database configuration - Docker environment
DB_CONFIG = {
    "host": "postgres",  # Docker container name
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def get_day_type_null_dates_from_db() -> List[date]:
    """ดึงวันที่ที่มี day_type เป็น -1 จากฐานข้อมูล"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT feature_date 
            FROM features 
            WHERE day_type = -1 
            ORDER BY feature_date
        """)
        
        results = cur.fetchall()
        null_dates = [row[0] for row in results]
        
        cur.close()
        conn.close()
        
        print(f"📅 พบวันที่ที่ยังไม่มีข้อมูล day_type: {len(null_dates)} วัน")
        if null_dates:
            print(f"   ช่วง: {null_dates[0]} ถึง {null_dates[-1]}")
        
        return null_dates
        
    except Exception as e:
        print(f"❌ Error getting day_type null dates: {e}")
        return []

def download_holiday_calendar() -> Optional[pd.DataFrame]:
    """ดาวน์โหลดปฏิทินวันหยุดจาก myhora.com"""
    
    current_year = datetime.now().year + 543  # Buddhist year
    download_url = f"https://www.myhora.com/calendar/ical/holiday.aspx?{current_year}.csv"
    
    # สร้างโฟลเดอร์ชั่วคราวใน container
    folder = "/tmp/holiday_data"
    csv_filename = f'myhora-holiday-calendar-{current_year}.csv'
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    csv_path = os.path.join(folder, csv_filename)
    
    print(f"📥 กำลังดาวน์โหลดปฏิทินวันหยุด ปี {current_year}")
    
    try:
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()
        
        with open(csv_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ ดาวน์โหลดสำเร็จ: {csv_path}")
        
        # อ่านไฟล์
        holiday_df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"📊 โหลดข้อมูลวันหยุด: {len(holiday_df)} รายการ")
        
        if len(holiday_df) > 0:
            print(f"📋 คอลัมน์: {holiday_df.columns.tolist()}")
            print("📋 ตัวอย่างข้อมูล:")
            for _, row in holiday_df.head(3).iterrows():
                start_date = row.get('Start Date', 'N/A')
                summary = row.get('Summary', 'N/A')
                print(f"   {start_date}: {summary}")
        
        return holiday_df
        
    except Exception as e:
        print(f"❌ ไม่สามารถดาวน์โหลดปฏิทินวันหยุด: {e}")
        print("⚠️ จะใช้ข้อมูลเทศกาลเท่านั้น")
        return pd.DataFrame(columns=['Start Date', 'Summary'])

def get_second_saturday(year: int, month: int) -> str:
    """หาเสาร์ที่ 2 ของเดือน (วันเด็ก)"""
    cal = calendar.monthcalendar(year, month)
    
    # หาเสาร์แรกในปฏิทิน
    first_week = cal[0]
    if first_week[calendar.SATURDAY] != 0:
        # เสาร์แรกอยู่ในสัปดาห์แรก
        second_saturday = cal[1][calendar.SATURDAY]
    else:
        # เสาร์แรกอยู่ในสัปดาห์ที่สอง
        second_saturday = cal[1][calendar.SATURDAY]
        if second_saturday == 0:
            second_saturday = cal[2][calendar.SATURDAY]
    
    return f"{year:04d}-{month:02d}-{second_saturday:02d}"

def get_festival_dates(year: int) -> Dict[str, str]:
    """กำหนดวันเทศกาลต่างๆ"""
    
    second_sat_jan = get_second_saturday(year, 1)
    
    festivals = {
        "วันเด็ก": second_sat_jan,
        "วาเลนไทน์": f"{year}-02-14",
        "สงกรานต์ วันที่ 1": f"{year}-04-13",
        "สงกรานต์ วันที่ 2": f"{year}-04-14", 
        "สงกรานต์ วันที่ 3": f"{year}-04-15",
        "คริสต์มาสอีฟ": f"{year}-12-24",
        "คริสต์มาส": f"{year}-12-25",
        "วันสิ้นปี": f"{year}-12-31",
    }
    
    print("🎉 เทศกาลที่กำหนด:")
    for name, date in festivals.items():
        print(f"   {name}: {date}")
    
    return festivals

def classify_day_types(dates: List[date], holiday_df: pd.DataFrame) -> pd.DataFrame:
    """จำแนกประเภทวันสำหรับวันที่ที่ระบุ"""
    
    if not dates:
        return pd.DataFrame()
    
    # แยกปีออกมาจากวันที่
    years = set(d.year for d in dates)
    
    print(f"🗓️ กำลังจำแนกประเภทวันสำหรับ {len(dates)} วัน")
    print(f"📅 ปีที่ครอบคลุม: {sorted(years)}")
    
    # สร้าง set ของวันหยุด
    holiday_dates = set()
    if len(holiday_df) > 0 and 'Start Date' in holiday_df.columns:
        for _, row in holiday_df.iterrows():
            try:
                date_str = str(row['Start Date']).strip()
                if len(date_str) == 8:  # Format: 20250101
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                    holiday_dates.add(date_obj.strftime('%Y-%m-%d'))
                elif '-' in date_str:  # Format: 2025-01-01
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    holiday_dates.add(date_obj.strftime('%Y-%m-%d'))
            except Exception:
                continue
    
    print(f"🏖️ โหลดวันหยุดราชการ: {len(holiday_dates)} วัน")
    
    # รวบรวมวันเทศกาลจากทุกปี
    festival_dates = set()
    for year in years:
        year_festivals = get_festival_dates(year)
        festival_dates.update(year_festivals.values())
    
    print(f"🎉 วันเทศกาลทั้งหมด: {len(festival_dates)} วัน")
    
    # จำแนกประเภทแต่ละวัน
    day_type_data = []
    
    for date_obj in dates:
        date_str = date_obj.strftime('%Y-%m-%d')
        
        # ลำดับความสำคัญ: วันหยุด (0) > เทศกาล (2) > วันปกติ (1)
        if date_str in holiday_dates:
            day_type = 0
            day_category = "Holiday"
        elif date_str in festival_dates:
            day_type = 2
            day_category = "Festival"
        else:
            day_type = 1
            day_category = "Normal/Weekend"
        
        # ข้อมูลเสริม
        weekday = date_obj.strftime('%A')
        is_weekend = date_obj.weekday() >= 5  # Saturday=5, Sunday=6
        
        day_type_data.append({
            'feature_date': date_obj,
            'day_type': day_type,
            'day_category': day_category,
            'weekday': weekday,
            'is_weekend': is_weekend
        })
    
    result_df = pd.DataFrame(day_type_data)
    
    # สรุปผลการจำแนก
    print(f"\n📊 สรุปการจำแนกประเภทวัน:")
    if not result_df.empty:
        type_counts = result_df['day_category'].value_counts()
        for category, count in type_counts.items():
            day_type_num = result_df[result_df['day_category'] == category]['day_type'].iloc[0]
            print(f"   {category} (type {day_type_num}): {count} วัน")
        
        # แสดงตัวอย่าง
        print(f"\n📋 ตัวอย่างการจำแนก:")
        for day_type in [0, 1, 2]:
            examples = result_df[result_df['day_type'] == day_type].head(2)
            if len(examples) > 0:
                category = examples.iloc[0]['day_category']
                print(f"   {category} (type {day_type}):")
                for _, row in examples.iterrows():
                    print(f"     {row['feature_date']} ({row['weekday']})")
    
    return result_df

def update_day_type_in_db(day_type_data: pd.DataFrame) -> bool:
    """อัพเดต day_type ในฐานข้อมูล"""
    if day_type_data is None or day_type_data.empty:
        print("❌ ไม่มีข้อมูล day_type ให้อัพเดต")
        return False
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print(f"🔄 กำลังอัพเดต day_type สำหรับ {len(day_type_data)} วัน...")
        
        # อัพเดตทีละรายการ
        updated_count = 0
        for _, row in day_type_data.iterrows():
            feature_date = row['feature_date']
            day_type = int(row['day_type'])
            
            cur.execute("""
                UPDATE features 
                SET day_type = %s 
                WHERE feature_date = %s AND day_type = -1
            """, (day_type, feature_date))
            
            if cur.rowcount > 0:
                updated_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ อัพเดต day_type สำเร็จ: {updated_count}/{len(day_type_data)} รายการ")
        return True
        
    except Exception as e:
        print(f"❌ ข้อผิดพลาดในการอัพเดต day_type: {e}")
        return False

def main():
    """ฟังก์ชันหลัก - รองรับการเรียกใช้จาก Airflow"""
    print("📅 เริ่มต้นการอัพเดตข้อมูล Day Type (Airflow Mode)")
    
    # รับ arguments จาก Airflow (optional)
    if len(sys.argv) >= 4:
        postgres_db = sys.argv[1]  # จะได้รับแต่ไม่ใช้ เพราะใช้ DB_CONFIG แทน
        postgres_user = sys.argv[2]
        postgres_pwd = sys.argv[3]
        print(f"📊 รับ arguments จาก Airflow: db={postgres_db}, user={postgres_user}")
    else:
        print("📊 รันในโหมด standalone")
    
    # 1. ตรวจสอบวันที่ที่ยังไม่มีข้อมูล day_type
    null_day_type_dates = get_day_type_null_dates_from_db()
    if not null_day_type_dates:
        print("✅ ไม่มีวันที่ที่ต้องอัพเดต day_type")
        return True
    
    print(f"📅 พบ {len(null_day_type_dates)} วันที่ต้องเติมข้อมูล day_type")
    
    # 2. ดาวน์โหลดปฏิทินวันหยุด (อย่างน้อย 1 ครั้ง)
    holiday_df = download_holiday_calendar()
    
    # 3. จำแนกประเภทวัน
    day_type_data = classify_day_types(null_day_type_dates, holiday_df)
    if day_type_data is None or day_type_data.empty:
        print("❌ ไม่สามารถจำแนกประเภทวันได้")
        return False
    
    # 4. อัพเดตข้อมูลในฐานข้อมูล
    success = update_day_type_in_db(day_type_data)
    
    if success:
        print("🎉 การอัพเดตข้อมูล day_type เสร็จสิ้นสำเร็จ!")
        
        # แสดงสรุปข้อมูล
        print(f"\n📊 สรุปข้อมูล day_type ที่อัพเดต:")
        print(f"   - จำนวนวัน: {len(day_type_data)}")
        
        # สถิติตามประเภทวัน
        type_counts = day_type_data['day_category'].value_counts()
        for category, count in type_counts.items():
            day_type_num = day_type_data[day_type_data['day_category'] == category]['day_type'].iloc[0]
            print(f"   - {category} (type {day_type_num}): {count} วัน")
        
        # วันหยุดและเทศกาลที่สำคัญ
        special_days = day_type_data[day_type_data['day_type'].isin([0, 2])]
        if not special_days.empty:
            print(f"\n🎯 วันพิเศษที่อัพเดต:")
            for _, row in special_days.head(10).iterrows():
                print(f"   {row['feature_date']}: {row['day_category']} (type {row['day_type']})")
        
        return True
    else:
        print("❌ การอัพเดตข้อมูล day_type ล้มเหลว")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)