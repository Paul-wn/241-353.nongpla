#!/usr/bin/env python3
"""
Rain Data Collection Spark Application - Functional Version

ใช้งานร่วมกับ Airflow DAG สำหรับดึงข้อมูลฝนจาก OpenMeteo API
และอัพเดต rain_average เข้า PostgreSQL features table

Based on: rain_collecting_functional.py (working version)
Adapted for: Spark + Airflow environment
Purpose: Update rain_average column for newly added MOT data
"""

import psycopg2
import pandas as pd
import requests
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, date, timedelta
import sys
from typing import Optional, Dict, List, Any, Tuple
import numpy as np

# Database configuration - Docker environment
DB_CONFIG = {
    "host": "postgres",  # Docker container name
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def get_null_rain_dates_from_db() -> List[date]:
    """ดึงวันที่ที่มี rain_average เป็น NULL จากฐานข้อมูล"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT feature_date 
            FROM features 
            WHERE rain_average IS NULL 
            ORDER BY feature_date
        """)
        
        results = cur.fetchall()
        null_dates = [row[0] for row in results]
        
        cur.close()
        conn.close()
        
        print(f"📅 พบวันที่ที่ยังไม่มีข้อมูลฝน: {len(null_dates)} วัน")
        if null_dates:
            print(f"   ช่วง: {null_dates[0]} ถึง {null_dates[-1]}")
        
        return null_dates
        
    except Exception as e:
        print(f"❌ Error getting null rain dates: {e}")
        return []

def setup_openmeteo_client():
    """ตั้งค่า OpenMeteo API client"""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def get_bangkok_locations():
    """ได้รับรายการตำแหน่งสถานีรถไฟฟ้าในกรุงเทพฯ"""
    return [
        # BTS Silom Line
        {"name": "BTS Siam", "lat": 13.7459, "lon": 100.5340},
        {"name": "BTS Chong Nonsi", "lat": 13.7236, "lon": 100.5310},
        {"name": "BTS Surasak", "lat": 13.7197, "lon": 100.5156},
        {"name": "BTS Saphan Taksin", "lat": 13.7197, "lon": 100.5089},
        {"name": "BTS Krung Thon Buri", "lat": 13.7211, "lon": 100.4944},
        {"name": "BTS Wongwian Yai", "lat": 13.7244, "lon": 100.4756},
        {"name": "BTS Bang Wa", "lat": 13.7250, "lon": 100.4033},
        
        # BTS Sukhumvit Line
        {"name": "BTS Phrom Phong", "lat": 13.7308, "lon": 100.5697},
        {"name": "BTS Thong Lo", "lat": 13.7244, "lon": 100.5797},
        {"name": "BTS Ekkamai", "lat": 13.7197, "lon": 100.5889},
        {"name": "BTS Phra Khanong", "lat": 13.7178, "lon": 100.5950},
        {"name": "BTS On Nut", "lat": 13.7056, "lon": 100.6014},
        {"name": "BTS Bang Chak", "lat": 13.6936, "lon": 100.6078},
        {"name": "BTS Bearing", "lat": 13.6631, "lon": 100.6069},
        {"name": "BTS Samrong", "lat": 13.6464, "lon": 100.6081},
        {"name": "BTS Mo Chit", "lat": 13.8028, "lon": 100.5544},
        {"name": "BTS Saphan Phut", "lat": 13.8139, "lon": 100.5556},
        {"name": "BTS Lat Phrao", "lat": 13.8167, "lon": 100.6108},
        
        # MRT Blue Line
        {"name": "MRT Hua Lamphong", "lat": 13.7375, "lon": 100.5169},
        {"name": "MRT Khlong Toei", "lat": 13.7208, "lon": 100.5442},
        {"name": "MRT Queen Sirikit", "lat": 13.7286, "lon": 100.5561},
        {"name": "MRT Sukhumvit", "lat": 13.7381, "lon": 100.5603},
        {"name": "MRT Phetchaburi", "lat": 13.7486, "lon": 100.5644},
        {"name": "MRT Phra Ram 9", "lat": 13.7597, "lon": 100.5661},
        {"name": "MRT Sutthisan", "lat": 13.7800, "lon": 100.5717},
        {"name": "MRT Huai Khwang", "lat": 13.7675, "lon": 100.5744},
        {"name": "MRT Thailand Cultural Centre", "lat": 13.7692, "lon": 100.5481},
        {"name": "MRT Chatuchak Park", "lat": 13.7981, "lon": 100.5522},
        {"name": "MRT Bang Sue", "lat": 13.8200, "lon": 100.5344},
        {"name": "MRT Tao Poon", "lat": 13.8064, "lon": 100.5206},
        {"name": "MRT Bang Pho", "lat": 13.7892, "lon": 100.5139},
        {"name": "MRT Lat Mayom", "lat": 13.7636, "lon": 100.4839},
        
        # MRT Purple Line
        {"name": "MRT Khlong Bang Phai", "lat": 13.8333, "lon": 100.5206},
        {"name": "MRT Talad Bang Yai", "lat": 13.8519, "lon": 100.5367},
        {"name": "MRT Sam Yaek Bang Yai", "lat": 13.8656, "lon": 100.5447},
        {"name": "MRT Bang Phlu", "lat": 13.8797, "lon": 100.5542},
        {"name": "MRT Bang Rak Yai", "lat": 13.8953, "lon": 100.5636},
        {"name": "MRT Tha It", "lat": 13.9097, "lon": 100.5722},
        
        # Airport Rail Link (ARL)
        {"name": "ARL Phaya Thai", "lat": 13.7775, "lon": 100.5344},
        {"name": "ARL Ratchaprarop", "lat": 13.7519, "lon": 100.5414},
        {"name": "ARL Makkasan", "lat": 13.7450, "lon": 100.5631},
        {"name": "ARL Ramkhamhaeng", "lat": 13.7639, "lon": 100.6181},
        {"name": "ARL Hua Mak", "lat": 13.7519, "lon": 100.6456},
        {"name": "ARL Ban Thap Chang", "lat": 13.7297, "lon": 100.6756},
        {"name": "ARL Lat Krabang", "lat": 13.7308, "lon": 100.7542},
        {"name": "ARL Suvarnabhumi", "lat": 13.6900, "lon": 100.7500},
        
        # Major Transport Hubs
        {"name": "Victory Monument", "lat": 13.7650, "lon": 100.5375},
        {"name": "Chatuchak Weekend Market", "lat": 13.7981, "lon": 100.5522},
        {"name": "Central World", "lat": 13.7467, "lon": 100.5400},
        {"name": "MBK Center", "lat": 13.7444, "lon": 100.5306},
        {"name": "Terminal 21", "lat": 13.7381, "lon": 100.5603},
        {"name": "EmQuartier", "lat": 13.7308, "lon": 100.5697},
        {"name": "ICONSIAM", "lat": 13.7267, "lon": 100.5089},
        {"name": "Central Ladprao", "lat": 13.8167, "lon": 100.6108},
    ]

def fetch_rain_data_for_dates(dates: List[date], openmeteo_client) -> Optional[pd.DataFrame]:
    """ดึงข้อมูลฝนสำหรับวันที่ที่ระบุ"""
    if not dates:
        return None
    
    # กำหนด date range ที่ครอบคลุมวันที่ทั้งหมด
    start_date = min(dates)
    end_date = max(dates) + timedelta(days=1)
    
    print(f"🌧️ กำลังดึงข้อมูลฝน: {start_date} ถึง {end_date}")
    print(f"📊 จำนวนวันที่ต้องการ: {len(dates)} วัน")
    
    bangkok_locations = get_bangkok_locations()
    url = "https://api.open-meteo.com/v1/forecast"
    
    all_data = []
    
    # ดึงข้อมูลจากหลายจุดในกรุงเทพฯ
    for i, location in enumerate(bangkok_locations):
        print(f"🔍 กำลังดึงข้อมูลจากจุดที่ {i+1}/{len(bangkok_locations)}: {location['name']}")
        
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "daily": "rain_sum",
            "timezone": "Asia/Bangkok",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }
        
        try:
            responses = openmeteo_client.weather_api(url, params=params)
            response = responses[0]
            
            # ประมวลผลข้อมูลรายวัน
            daily = response.Daily()
            daily_rain_sum = daily.Variables(0).ValuesAsNumpy()
            
            daily_data = {
                "date": pd.date_range(
                    start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                    end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                    freq = pd.Timedelta(seconds = daily.Interval()),
                    inclusive = "left"
                ),
                "location": location["name"],
                "latitude": location["lat"],
                "longitude": location["lon"],
                "rain_sum": daily_rain_sum
            }
            
            location_df = pd.DataFrame(data = daily_data)
            all_data.append(location_df)
            
        except Exception as e:
            print(f"⚠️ ข้ามจุด {location['name']}: {e}")
            continue
    
    if not all_data:
        print("❌ ไม่สามารถดึงข้อมูลฝนได้")
        return None
    
    # รวมข้อมูลทั้งหมด
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # คำนวณค่าเฉลี่ยรายวัน
    daily_average = combined_df.groupby('date')['rain_sum'].agg(['mean']).reset_index()
    daily_average.columns = ['date', 'rain_average']
    
    # แปลงวันที่เป็น date object
    daily_average['date'] = daily_average['date'].dt.date
    
    # กรองเฉพาะวันที่ที่ต้องการ
    daily_average = daily_average[daily_average['date'].isin(dates)]
    
    print(f"✅ ดึงข้อมูลฝนเสร็จ: {len(daily_average)} วัน")
    
    # แสดงตัวอย่างข้อมูล
    print("📊 ตัวอย่างข้อมูลฝน (5 วันแรก):")
    for _, row in daily_average.head().iterrows():
        print(f"  {row['date']}: {row['rain_average']:.2f} mm")
    
    return daily_average

def update_rain_average_in_db(rain_data: pd.DataFrame) -> bool:
    """อัพเดต rain_average ในฐานข้อมูล"""
    if rain_data is None or rain_data.empty:
        print("❌ ไม่มีข้อมูลฝนให้อัพเดต")
        return False
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print(f"🔄 กำลังอัพเดต rain_average สำหรับ {len(rain_data)} วัน...")
        
        # อัพเดตทีละรายการ
        updated_count = 0
        for _, row in rain_data.iterrows():
            date_str = row['date']
            rain_avg = float(row['rain_average'])
            
            cur.execute("""
                UPDATE features 
                SET rain_average = %s 
                WHERE feature_date = %s AND rain_average IS NULL
            """, (rain_avg, date_str))
            
            if cur.rowcount > 0:
                updated_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ อัพเดตข้อมูลฝนสำเร็จ: {updated_count}/{len(rain_data)} รายการ")
        return True
        
    except Exception as e:
        print(f"❌ ข้อผิดพลาดในการอัพเดต: {e}")
        return False

def main():
    """ฟังก์ชันหลัก - รองรับการเรียกใช้จาก Airflow"""
    print("🌧️ เริ่มต้นการอัพเดตข้อมูลฝน (Airflow Mode)")
    
    # รับ arguments จาก Airflow (optional)
    if len(sys.argv) >= 4:
        postgres_db = sys.argv[1]  # จะได้รับแต่ไม่ใช้ เพราะใช้ DB_CONFIG แทน
        postgres_user = sys.argv[2]
        postgres_pwd = sys.argv[3]
        print(f"📊 รับ arguments จาก Airflow: db={postgres_db}, user={postgres_user}")
    else:
        print("📊 รันในโหมด standalone")
    
    # 1. ตรวจสอบวันที่ที่ยังไม่มีข้อมูลฝน
    null_rain_dates = get_null_rain_dates_from_db()
    if not null_rain_dates:
        print("✅ ไม่มีวันที่ที่ต้องอัพเดตข้อมูลฝน")
        return True
    
    print(f"📅 พบ {len(null_rain_dates)} วันที่ต้องเติมข้อมูลฝน")
    
    # 2. ตั้งค่า OpenMeteo client
    try:
        openmeteo_client = setup_openmeteo_client()
        print("✅ เชื่อมต่อ OpenMeteo API สำเร็จ")
    except Exception as e:
        print(f"❌ ไม่สามารถเชื่อมต่อ OpenMeteo API: {e}")
        return False
    
    # 3. ดึงข้อมูลฝน
    rain_data = fetch_rain_data_for_dates(null_rain_dates, openmeteo_client)
    if rain_data is None or rain_data.empty:
        print("❌ ไม่สามารถดึงข้อมูลฝนได้")
        return False
    
    # 4. อัพเดตข้อมูลในฐานข้อมูล
    success = update_rain_average_in_db(rain_data)
    
    if success:
        print("🎉 การอัพเดตข้อมูลฝนเสร็จสิ้นสำเร็จ!")
        
        # แสดงสรุปข้อมูล
        print(f"\n📊 สรุปข้อมูลฝนที่อัพเดต:")
        print(f"   - จำนวนวัน: {len(rain_data)}")
        print(f"   - ค่าเฉลี่ย: {rain_data['rain_average'].mean():.2f} mm")
        print(f"   - ต่ำสุด: {rain_data['rain_average'].min():.2f} mm")
        print(f"   - สูงสุด: {rain_data['rain_average'].max():.2f} mm")
        
        # จำแนกตามระดับฝน
        no_rain = len(rain_data[rain_data['rain_average'] < 0.1])
        light_rain = len(rain_data[(rain_data['rain_average'] >= 0.1) & (rain_data['rain_average'] <= 10.0)])
        moderate_rain = len(rain_data[(rain_data['rain_average'] > 10.0) & (rain_data['rain_average'] <= 35.0)])
        heavy_rain = len(rain_data[rain_data['rain_average'] > 35.0])
        
        print(f"   - ไม่มีฝน (<0.1mm): {no_rain} วัน")
        print(f"   - ฝนเบา (0.1-10mm): {light_rain} วัน") 
        print(f"   - ฝนปานกลาง (10-35mm): {moderate_rain} วัน")
        print(f"   - ฝนหนัก (>35mm): {heavy_rain} วัน")
        
        return True
    else:
        print("❌ การอัพเดตข้อมูลฝนล้มเหลว")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)