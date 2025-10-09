"""
Weekly MOT + Rain + Day Type Data Collection DAG with Quality Checks v2.0

This DAG orchestrates a comprehensive data collection pipeline with quality validation:
- MOT transportation data collection with anomaly detection
- Rain weather data enrichment from 40+ Bangkok stations
- Day type classification (workday/holiday/festival)
- Multi-stage quality checks with intelligent branching logic
- XCom data sharing for comprehensive reporting

Author: Data Engineering Team
Created: October 2025
Updated: October 2025 (v2.0 - Fixed trigger rules and branching)
Schedule: Manual trigger for testing (set to "0 2 * * 0" for production)

Features:
- BranchPythonOperator for conditional workflow (FIXED trigger rules)
- XCom for sharing quality metrics between tasks
- Comprehensive data validation at each stage
- Anomaly detection with realistic thresholds (BTS up to 2M passengers)
- Missing data alerts and zero-value detection
- Automatic retry with improved error handling

Improvements in v2.0:
- Fixed trigger_rule issues with BranchPythonOperator
- Added none_failed_min_one_success for downstream tasks
- Improved DAG ID to avoid legacy DAG run conflicts
- Enhanced error handling and retry logic

Usage:
1. Enable the DAG in Airflow UI (new DAG ID: weekly-mot-rain-data-collecting-v2)
2. Trigger manually - should work from first run
3. Monitor logs for data collection and quality reports  
4. Check PostgreSQL features table for validated records
5. View XCom in Airflow UI for quality reports
"""

import os
import psycopg2
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/assets/jars/postgresql-42.2.6.jar"

# Database connection parameters
postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

# Database connection config for Python tasks
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),  # เริ่มต้นวันที่ชัดเจน
    "email": ["data-team@company.com"],
    "email_on_failure": False,  # ปิดอีเมลสำหรับการทดสอบ
    "email_on_retry": False,
    "retries": 2,  # เพิ่ม retries เพื่อรองรับ dependency issues
    "retry_delay": timedelta(minutes=1),  # ลด retry delay
    "max_active_runs": 1
}

dag = DAG(
    dag_id="weekly-mot-rain-data-collecting-v2",  # เปลี่ยน DAG ID ใหม่เพื่อหลีกเลี่ยง DAG runs เก่า
    description="Weekly MOT + Rain + Day Type Data Collection DAG with Quality Checks v2.0",
    default_args=default_args,
    schedule_interval="0 2 * * 0",  # ทุกวันอาทิตย์เวลา 02:00 น. (เปลี่ยนเป็น None สำหรับการทดสอบ)
    catchup=False,
    max_active_runs=1,
    tags=["mot", "rain", "day-type", "quality-checks", "branching", "xcom", "v2", "production"]
)

###############################################
# Quality Check Functions
###############################################

def check_mot_data_quality(**context):
    """
    ตรวจสอบคุณภาพข้อมูล MOT หลังจาก Collection
    - ตรวจสอบความสมบูรณ์ของข้อมูลทุกสาย
    - หา Anomalies และ Missing data
    - ส่งผล XCom สำหรับ tasks ถัดไป
    
    Realistic Transport Data Ranges:
    - BTS: 800,000 - 2,000,000 ผู้โดยสาร/วัน (สายหลักในกทม.)
    - MRT Blue: 600,000 - 1,500,000 ผู้โดยสาร/วัน
    - MRT Purple: 200,000 - 800,000 ผู้โดยสาร/วัน  
    - ARL: 50,000 - 200,000 ผู้โดยสาร/วัน
    - SRT Red: 100,000 - 300,000 ผู้โดยสาร/วัน
    - MRT Yellow/Pink: 100,000 - 500,000 ผู้โดยสาร/วัน (สายใหม่)
    """
    import json
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # ตรวจสอบข้อมูลล่าสุด 7 วัน
    quality_check = {
        "task": "mot_quality_check",
        "timestamp": datetime.now().isoformat(),
        "records_processed": 0,
        "missing_data": {},
        "anomalies": [],
        "quality_score": 0.0,
        "status": "PASS"
    }
    
    try:
        # 1. นับจำนวน records ที่เพิ่งอัปเดต
        cur.execute("""
            SELECT COUNT(*) FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        quality_check["records_processed"] = cur.fetchone()[0]
        
        # 2. ตรวจสอบ Missing data และ Zero values ในแต่ละสาย
        transport_columns = ['arl', 'bts', 'mrt_blue', 'mrt_purple', 'mrt_pink', 'srt_red', 'mrt_yellow']
        
        for column in transport_columns:
            # Missing หรือ NULL values
            cur.execute(f"""
                SELECT feature_date FROM features 
                WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
                AND {column} IS NULL
                ORDER BY feature_date
            """)
            null_dates = [str(row[0]) for row in cur.fetchall()]
            
            # Zero values (น่าสงสัย - ระบบขนส่งไม่น่าจะมีผู้โดยสาร 0)
            cur.execute(f"""
                SELECT feature_date FROM features 
                WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
                AND {column} = 0
                ORDER BY feature_date
            """)
            zero_dates = [str(row[0]) for row in cur.fetchall()]
            
            if null_dates or zero_dates:
                issues = []
                if null_dates:
                    issues.extend([f"{date} (NULL)" for date in null_dates])
                if zero_dates:
                    issues.extend([f"{date} (ZERO)" for date in zero_dates])
                quality_check["missing_data"][column] = issues
        
        # 3. ตรวจสอบ Anomalies (ค่าสูงผิดปกติ) - ปรับตาม realistic values
        anomaly_thresholds = {
            'bts': 2000000,        # BTS อาจมีผู้โดยสาร 1-2 ล้าน/วัน (ปกติ)
            'mrt_blue': 1500000,   # MRT สายน้ำเงินค่อนข้างคึกคัก
            'mrt_purple': 800000,  # MRT สายม่วงปานกลาง
            'arl': 200000,         # ARL ไม่เยอะมาก
            'srt_red': 300000,     # SRT สายแดงปานกลาง
            'mrt_yellow': 500000,  # MRT สายเหลืองใหม่ๆ
            'mrt_pink': 400000     # MRT สายชมพูใหม่ๆ
        }
        
        for column in transport_columns:
            threshold = anomaly_thresholds.get(column, 1000000)  # default 1M
            cur.execute(f"""
                SELECT feature_date, {column} FROM features 
                WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
                AND ({column} > {threshold} OR {column} < 0)  -- เกิน threshold หรือติดลบ
            """)
            anomalies = cur.fetchall()
            for date, value in anomalies:
                if value < 0:
                    quality_check["anomalies"].append(f"{column} = {value:,.0f} (negative) on {date}")
                else:
                    quality_check["anomalies"].append(f"{column} = {value:,.0f} (>{threshold:,}) on {date}")
        
        # 4. คำนวณ Quality Score
        total_expected_records = 7 * len(transport_columns)  # 7 วัน × 7 สาย
        total_missing = sum(len(dates) for dates in quality_check["missing_data"].values())
        quality_check["quality_score"] = max(0, (total_expected_records - total_missing) / total_expected_records)
        
        # 5. กำหนด Status
        if quality_check["quality_score"] < 0.8:
            quality_check["status"] = "FAIL"
        elif quality_check["anomalies"] or total_missing > 0:
            quality_check["status"] = "WARNING"
        
        print(f"📊 MOT Data Quality Report:")
        print(f"   Records Processed: {quality_check['records_processed']}")
        print(f"   Quality Score: {quality_check['quality_score']:.2%}")
        
        if quality_check['missing_data']:
            print(f"   ⚠️ Data Issues:")
            for line, issues in quality_check['missing_data'].items():
                print(f"      {line}: {issues}")
        else:
            print(f"   ✅ No missing data detected")
            
        if quality_check['anomalies']:
            print(f"   🔍 Anomalies Found:")
            for anomaly in quality_check['anomalies']:
                print(f"      {anomaly}")
        else:
            print(f"   ✅ No anomalies detected")
            
        print(f"   📈 Overall Status: {quality_check['status']}")
        
    except Exception as e:
        quality_check["status"] = "FAIL"
        quality_check["error"] = str(e)
        print(f"❌ MOT Quality Check Failed: {e}")
    
    finally:
        conn.close()
    
    # ส่งผลลัพธ์ผ่าน XCom
    context['task_instance'].xcom_push(key='mot_quality_result', value=quality_check)
    
    # Return branch decision
    if quality_check["status"] == "FAIL":
        return "mot_quality_failed"
    else:
        return "collect_rain_weather_data"


def check_rain_data_quality(**context):
    """
    ตรวจสอบคุณภาพข้อมูล Rain หลังจาก Collection
    """
    import json
    
    # ดึงผลลัพธ์จาก MOT Quality Check
    mot_result = context['task_instance'].xcom_pull(key='mot_quality_result')
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    quality_check = {
        "task": "rain_quality_check", 
        "timestamp": datetime.now().isoformat(),
        "records_processed": 0,
        "null_rain_remaining": 0,
        "anomalies": [],
        "quality_score": 0.0,
        "status": "PASS"
    }
    
    try:
        # 1. ตรวจสอบ records ที่ยังเป็น NULL
        cur.execute("""
            SELECT COUNT(*) FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND rain_average IS NULL
        """)
        quality_check["null_rain_remaining"] = cur.fetchone()[0]
        
        # 2. นับ records ที่มี rain data
        cur.execute("""
            SELECT COUNT(*) FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND rain_average IS NOT NULL
        """)
        quality_check["records_processed"] = cur.fetchone()[0]
        
        # 3. ตรวจสอบค่าฝนผิดปกติ
        cur.execute("""
            SELECT feature_date, rain_average FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND (rain_average < 0 OR rain_average > 200)
        """)
        anomalies = cur.fetchall()
        for date, value in anomalies:
            quality_check["anomalies"].append(f"rain = {value:.1f}mm on {date}")
        
        # 4. คำนวณ Quality Score
        total_records = mot_result.get("records_processed", 0) if mot_result else 7
        if total_records > 0:
            quality_check["quality_score"] = quality_check["records_processed"] / total_records
        
        # 5. กำหนด Status
        if quality_check["null_rain_remaining"] > 0:
            quality_check["status"] = "WARNING"
        if quality_check["quality_score"] < 0.8:
            quality_check["status"] = "FAIL"
            
        print(f"🌧️ Rain Data Quality Report:")
        print(f"   Records with Rain Data: {quality_check['records_processed']}")
        print(f"   NULL Rain Remaining: {quality_check['null_rain_remaining']}")
        print(f"   Quality Score: {quality_check['quality_score']:.2%}")
        print(f"   Anomalies: {quality_check['anomalies']}")
        print(f"   Status: {quality_check['status']}")
        
    except Exception as e:
        quality_check["status"] = "FAIL"
        quality_check["error"] = str(e)
        print(f"❌ Rain Quality Check Failed: {e}")
    
    finally:
        conn.close()
    
    # ส่งผลลัพธ์ผ่าน XCom
    context['task_instance'].xcom_push(key='rain_quality_result', value=quality_check)
    
    # Return branch decision
    if quality_check["status"] == "FAIL":
        return "rain_quality_failed"
    else:
        return "collect_day_type_data"


def check_day_type_data_quality(**context):
    """
    ตรวจสอบคุณภาพข้อมูล Day Type หลังจาก Collection
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    quality_check = {
        "task": "day_type_quality_check",
        "timestamp": datetime.now().isoformat(),
        "records_processed": 0,
        "pending_day_type_remaining": 0,
        "classification_errors": [],
        "quality_score": 0.0,
        "status": "PASS"
    }
    
    try:
        # 1. ตรวจสอบ day_type ที่ยังเป็น -1
        cur.execute("""
            SELECT COUNT(*) FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND day_type = -1
        """)
        quality_check["pending_day_type_remaining"] = cur.fetchone()[0]
        
        # 2. นับ records ที่มี day_type แล้ว
        cur.execute("""
            SELECT COUNT(*) FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND day_type != -1
        """)
        quality_check["records_processed"] = cur.fetchone()[0]
        
        # 3. ตรวจสอบ Logic Validation (วันเสาร์-อาทิตย์ควรเป็น day_type = 1)
        cur.execute("""
            SELECT feature_date, day_type, dow FROM features 
            WHERE feature_date >= CURRENT_DATE - INTERVAL '7 days'
            AND dow IN (6, 0)  -- เสาร์, อาทิตย์
            AND day_type NOT IN (1, 2)  -- ควรเป็นวันหยุด
        """)
        weekend_errors = cur.fetchall()
        for date, day_type, dow in weekend_errors:
            day_name = "Saturday" if dow == 6 else "Sunday"
            quality_check["classification_errors"].append(f"{day_name} {date} classified as day_type={day_type}")
        
        # 4. คำนวณ Quality Score
        mot_result = context['task_instance'].xcom_pull(key='mot_quality_result')
        total_records = mot_result.get("records_processed", 0) if mot_result else 7
        if total_records > 0:
            quality_check["quality_score"] = quality_check["records_processed"] / total_records
        
        # 5. กำหนด Status
        if quality_check["pending_day_type_remaining"] > 0:
            quality_check["status"] = "WARNING"
        if quality_check["classification_errors"]:
            quality_check["status"] = "WARNING"
        if quality_check["quality_score"] < 0.8:
            quality_check["status"] = "FAIL"
            
        print(f"📅 Day Type Data Quality Report:")
        print(f"   Records with Day Type: {quality_check['records_processed']}")
        print(f"   Pending Day Types: {quality_check['pending_day_type_remaining']}")
        print(f"   Classification Errors: {quality_check['classification_errors']}")
        print(f"   Quality Score: {quality_check['quality_score']:.2%}")
        print(f"   Status: {quality_check['status']}")
        
    except Exception as e:
        quality_check["status"] = "FAIL"
        quality_check["error"] = str(e)
        print(f"❌ Day Type Quality Check Failed: {e}")
    
    finally:
        conn.close()
    
    # ส่งผลลัพธ์ผ่าน XCom
    context['task_instance'].xcom_push(key='day_type_quality_result', value=quality_check)
    
    # Return branch decision
    if quality_check["status"] == "FAIL":
        return "day_type_quality_failed"
    else:
        return "final_quality_validation"


def generate_final_report(**context):
    """
    สร้างรายงานสรุปคุณภาพข้อมูลทั้งหมด
    """
    # ดึงผลลัพธ์จากทุก Quality Check
    mot_result = context['task_instance'].xcom_pull(key='mot_quality_result')
    rain_result = context['task_instance'].xcom_pull(key='rain_quality_result') 
    day_type_result = context['task_instance'].xcom_pull(key='day_type_quality_result')
    
    final_report = {
        "pipeline": "MOT-Rain-DayType Data Collection",
        "timestamp": datetime.now().isoformat(),
        "overall_status": "PASS",
        "stages": {
            "mot": mot_result,
            "rain": rain_result, 
            "day_type": day_type_result
        }
    }
    
    # กำหนด Overall Status
    statuses = [
        mot_result.get("status", "UNKNOWN") if mot_result else "UNKNOWN",
        rain_result.get("status", "UNKNOWN") if rain_result else "UNKNOWN", 
        day_type_result.get("status", "UNKNOWN") if day_type_result else "UNKNOWN"
    ]
    
    if "FAIL" in statuses:
        final_report["overall_status"] = "FAIL"
    elif "WARNING" in statuses:
        final_report["overall_status"] = "WARNING"
    
    print("=" * 60)
    print("🎯 FINAL PIPELINE QUALITY REPORT")
    print("=" * 60)
    print(f"Overall Status: {final_report['overall_status']}")
    print(f"Timestamp: {final_report['timestamp']}")
    print()
    
    for stage_name, result in final_report["stages"].items():
        if result:
            print(f"{stage_name.upper()} Stage:")
            print(f"  Status: {result.get('status', 'UNKNOWN')}")
            print(f"  Records: {result.get('records_processed', 0)}")
            print(f"  Quality Score: {result.get('quality_score', 0):.2%}")
            if result.get('anomalies'):
                print(f"  Anomalies: {len(result['anomalies'])}")
            print()
    
    # ส่งรายงานสุดท้ายผ่าน XCom
    context['task_instance'].xcom_push(key='final_report', value=final_report)

###############################################
# DAG Tasks
###############################################

# Start task
start_task = DummyOperator(
    task_id="start_weekly_data_collection",
    dag=dag
)


# MOT data collection task - ใจกลางของ workflow (ใช้ functional version)
mot_data_collection = SparkSubmitOperator(
    task_id="collect_mot_transport_data",
    application="/usr/local/spark/applications/mot_collecting.py",  # Functional version
    name="mot-data-collector-functional",
    conn_id=spark_conn,
    verbose=1,
    conf={
        # Simplified Spark configuration for Python script
        "spark.master": spark_master,
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.driver.extraClassPath": postgres_driver_jar,
        "spark.executor.extraClassPath": postgres_driver_jar
    },
    application_args=[postgres_db, postgres_user, postgres_pwd],  # ส่ง args ไปยัง script
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag,
)

# MOT Quality Check with Branching Decision
mot_quality_check = BranchPythonOperator(
    task_id="mot_quality_check",
    python_callable=check_mot_data_quality,
    dag=dag,
)

# Rain data collection task - ทำงานต่อเมื่อ MOT quality ผ่าน
rain_data_collection = SparkSubmitOperator(
    task_id="collect_rain_weather_data",
    application="/usr/local/spark/applications/rain_collecting.py",  # Rain functional version
    name="rain-data-collector-functional",
    conn_id=spark_conn,
    verbose=1,
    conf={
        # Simplified Spark configuration for Python script
        "spark.master": spark_master,
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.driver.extraClassPath": postgres_driver_jar,
        "spark.executor.extraClassPath": postgres_driver_jar
    },
    application_args=[postgres_db, postgres_user, postgres_pwd],  # ส่ง args ไปยัง script
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    trigger_rule="none_failed_min_one_success",  # รันได้แม้ upstream จะถูก SKIP
    dag=dag,
)

# Rain Quality Check with Branching Decision  
rain_quality_check = BranchPythonOperator(
    task_id="rain_quality_check",
    python_callable=check_rain_data_quality,
    dag=dag,
)

# Day type data collection task - ทำงานต่อเมื่อ Rain quality ผ่าน
day_type_data_collection = SparkSubmitOperator(
    task_id="collect_day_type_data",
    application="/usr/local/spark/applications/day_type_collecting.py",  # Day type functional version
    name="day-type-data-collector-functional",
    conn_id=spark_conn,
    verbose=1,
    conf={
        # Simplified Spark configuration for Python script
        "spark.master": spark_master,
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.driver.extraClassPath": postgres_driver_jar,
        "spark.executor.extraClassPath": postgres_driver_jar
    },
    application_args=[postgres_db, postgres_user, postgres_pwd],  # ส่ง args ไปยัง script
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    trigger_rule="none_failed_min_one_success",  # รันได้แม้ upstream จะถูก SKIP
    dag=dag,
)

# Day Type Quality Check with Branching Decision
day_type_quality_check = BranchPythonOperator(
    task_id="day_type_quality_check",
    python_callable=check_day_type_data_quality,
    dag=dag,
)

# Final Quality Validation - สร้างรายงานสรุป
final_quality_validation = PythonOperator(
    task_id="final_quality_validation",
    python_callable=generate_final_report,
    trigger_rule="none_failed_min_one_success",  # รันได้แม้ upstream จะถูก SKIP
    dag=dag,
)

# Success/Failure endpoints
pipeline_success = DummyOperator(
    task_id="pipeline_success",
    trigger_rule="none_failed_min_one_success",  # รันได้แม้ upstream จะถูก SKIP
    dag=dag
)

# Quality Check Failed Tasks
mot_quality_failed = DummyOperator(
    task_id="mot_quality_failed",
    dag=dag
)

rain_quality_failed = DummyOperator(
    task_id="rain_quality_failed", 
    dag=dag
)

day_type_quality_failed = DummyOperator(
    task_id="day_type_quality_failed",
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id="end_weekly_data_collection",
    trigger_rule="none_failed_min_one_success",  # รันได้แม้จะมี paths ที่ถูก SKIP
    dag=dag
)

###############################################
# Task Dependencies & Workflow
###############################################

# Define the comprehensive data collection workflow with Quality Checks & Branching
# 
# Flow: Start → MOT Collection → MOT Quality Check → Rain Collection → Rain Quality Check 
#       → Day Type Collection → Day Type Quality Check → Final Validation → Success → End
#       
# Quality Check Branching: Each stage can branch to PASS (continue) or FAIL (end pipeline)
# 
# 1. Start: Initialize the workflow
# 2. MOT Collection: Fetch new transportation data from MOT API (sets rain_average = NULL, day_type = -1)  
# 3. MOT Quality Check: Validate MOT data completeness, detect anomalies (BranchPythonOperator)
# 4. Rain Collection: Fill rain_average for records where it's NULL (if MOT quality passes)
# 5. Rain Quality Check: Validate rain data coverage and values (BranchPythonOperator)
# 6. Day Type Collection: Fill day_type for records where it's -1 (if Rain quality passes)
# 7. Day Type Quality Check: Validate day type classifications (BranchPythonOperator)  
# 8. Final Validation: Generate comprehensive quality report (PythonOperator with XCom)
# 9. Success/End: Complete the workflow with fully validated features table
# 
# XCom Usage: Quality metrics and results are shared between tasks for comprehensive reporting

# Complex Task Dependencies with Quality Checks & Branching
# 
# BranchPythonOperator Rules:
# - Each BranchPythonOperator must have direct downstream connections to ALL possible return task_ids
# - The returned task_id will be the ONLY task that runs next
# - Non-selected tasks will be marked as SKIPPED
#
# Trigger Rules Used:
# - Default (all_success): All upstream tasks must succeed
# - none_failed_min_one_success: At least one upstream succeeded, none failed (allows SKIPPED)
#
# Flow Diagram:
#
# start_task → mot_data_collection → mot_quality_check → [collect_rain_weather_data | mot_quality_failed]
#                                                       ↓                           ↓
#                                            rain_quality_check → [collect_day_type_data | rain_quality_failed]
#                                                                 ↓                        ↓
#                                                     day_type_quality_check → [final_quality_validation | day_type_quality_failed]
#                                                                              ↓                           ↓
#                                                                    pipeline_success → end_task ← ALL FAILED PATHS

# Main pipeline flow:
start_task >> mot_data_collection >> mot_quality_check

# MOT Quality Check branches (BranchPythonOperator returns either task_id)
mot_quality_check >> rain_data_collection
mot_quality_check >> mot_quality_failed

# Rain pipeline continuation
rain_data_collection >> rain_quality_check

# Rain Quality Check branches (BranchPythonOperator returns either task_id)  
rain_quality_check >> day_type_data_collection
rain_quality_check >> rain_quality_failed

# Day Type pipeline continuation
day_type_data_collection >> day_type_quality_check

# Day Type Quality Check branches (BranchPythonOperator returns either task_id)
day_type_quality_check >> final_quality_validation
day_type_quality_check >> day_type_quality_failed

# Final success path
final_quality_validation >> pipeline_success >> end_task

# All failed paths lead to end_task
mot_quality_failed >> end_task
rain_quality_failed >> end_task  
day_type_quality_failed >> end_task

###############################################
# DAG Documentation
###############################################

# เอกสารประกอบ:
# - โค้ดหลัก: 
#   * MOT: /usr/local/spark/applications/mot_collecting.py (ใช้ logic จาก functional script)
#   * Rain: /usr/local/spark/applications/rain_collecting.py (ใช้ logic จาก functional script)
#   * Day Type: /usr/local/spark/applications/day_type_collecting.py (ใช้ logic จาก functional script)
# - ฐานข้อมูล: PostgreSQL features table (host: postgres, db: airflow)
# - API: MOT Open Data, OpenMeteo Weather, MyHora Holiday Calendar
# - APIs: 
#   * MOT Data: https://datagov.mot.go.th/api/action/datastore_search (MOT Open Data)
#   * Weather Data: https://api.open-meteo.com/v1/forecast (OpenMeteo API)
# - กำหนดการ: ทุกวันอาทิตย์เวลา 02:00 น. (Cron: "0 2 * * 0")
# - วัตถุประสงค์: ดึงข้อมูลการขนส่ง + สภาพอากาศใหม่และอัปเดตเข้าสู่ระบบอัตโนมัติ
# - Features: 
#   * ขั้นที่ 1 (MOT): ดึงข้อมูลการขนส่งใหม่ + ตั้ง rain_average = NULL
#   * ขั้นที่ 2 (Rain): เติม rain_average สำหรับวันที่ที่เป็น NULL
#   * ตรวจสอบวันที่ล่าสุดในฐานข้อมูล
#   * ดึงเฉพาะข้อมูลใหม่จาก APIs
#   * การทำงานต่อเนื่อง: MOT → Rain → Validation
#   * Upsert ข้อมูลแบบ intelligent
