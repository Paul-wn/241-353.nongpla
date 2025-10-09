import os 
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
import calendar

now = time.localtime()
current_year = now.tm_year + 543  # Convert to Buddhist year (2568)
christian_year = now.tm_year  # Christian year (2025)
download_url = f"https://www.myhora.com/calendar/ical/holiday.aspx?{current_year}.csv"

folder = f'./'
csv_filename = f'myhora-holiday-calendar-{current_year}.csv'
print(f"Current Buddhist Year: {current_year}")
print(f"Current Christian Year: {christian_year}")

# Create folder if not exists
if not os.path.exists(folder):
    os.makedirs(folder)

csv_path = os.path.join(folder, csv_filename)

# Download holiday calendar if not already present
if not os.path.exists(csv_path):
    print("Downloading holiday calendar...")
    try:
        response = requests.get(download_url)
        response.raise_for_status()
        
        with open(csv_path, 'wb') as f:
            f.write(response.content)
        print(f"Holiday calendar downloaded: {csv_path}")
    except Exception as e:
        print(f"Failed to download holiday calendar: {e}")
        # Create empty DataFrame if download fails
        holiday_df = pd.DataFrame(columns=['Start Date', 'Summary'])
else:
    print(f"Using existing holiday calendar: {csv_path}")

# Read holiday data
try:
    holiday_df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print("Holiday data loaded successfully")
    print(f"Columns: {holiday_df.columns.tolist()}")
    if len(holiday_df) > 0:
        print(f"Sample holiday data:\n{holiday_df.head()}")
except Exception as e:
    print(f"Error reading holiday file: {e}")
    # Create empty DataFrame if file reading fails
    holiday_df = pd.DataFrame(columns=['Start Date', 'Summary'])

# Define date range: 2025-09-04 to 2025-09-30
start_date = datetime(2025, 9, 4)
end_date = datetime(2025, 9, 30)

# Generate all dates in range
date_range = []
current_date = start_date
while current_date <= end_date:
    date_range.append(current_date)
    current_date += timedelta(days=1)

print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"Total days: {len(date_range)}")

# Define festivals (priority 2)
def get_second_saturday(year, month):
    """Get the second Saturday of a given month and year"""
    cal = calendar.monthcalendar(year, month)
    # Find first Saturday
    first_week = cal[0]
    if first_week[calendar.SATURDAY] != 0:
        # First Saturday is in first week
        second_saturday = cal[1][calendar.SATURDAY]
    else:
        # First Saturday is in second week
        second_saturday = cal[1][calendar.SATURDAY]
        if second_saturday == 0:
            second_saturday = cal[2][calendar.SATURDAY]
    return f"{year:04d}-{month:02d}-{second_saturday:02d}"

second_sat_jan = get_second_saturday(christian_year, 1)

festivals = [
    ("วันเด็ก", second_sat_jan),
    ("วาเลนไทน์", f"{christian_year}-02-14"),
    ("สงกรานต์", f"{christian_year}-04-13"),
    ("สงกรานต์", f"{christian_year}-04-14"), 
    ("สงกรานต์", f"{christian_year}-04-15"),
    ("คริสต์มาสอีฟ", f"{christian_year}-12-24"),
    ("คริสต์มาส", f"{christian_year}-12-25"),
    ("วันสิ้นปี", f"{christian_year}-12-31"),
]

print("\nDefined festivals:")
for name, date in festivals:
    print(f"  {name}: {date}")

# Convert holiday dates to set for quick lookup
holiday_dates = set()
if len(holiday_df) > 0 and 'Start Date' in holiday_df.columns:
    for _, row in holiday_df.iterrows():
        try:
            # Handle different date formats
            date_str = str(row['Start Date']).strip()
            if len(date_str) == 8:  # Format: 20250101
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                holiday_dates.add(date_obj.strftime('%Y-%m-%d'))
            elif '-' in date_str:  # Format: 2025-01-01
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                holiday_dates.add(date_obj.strftime('%Y-%m-%d'))
        except Exception as e:
            continue

print(f"\nLoaded {len(holiday_dates)} holidays")

# Convert festival dates to set
festival_dates = set()
for name, date in festivals:
    festival_dates.add(date)

print(f"Defined {len(festival_dates)} festival dates")

# Create day type mapping
day_type_data = []

for date_obj in date_range:
    date_str = date_obj.strftime('%Y-%m-%d')
    
    # Priority 1: Holiday (0) - highest priority
    if date_str in holiday_dates:
        day_type = 0
        day_category = "Holiday"
    # Priority 2: Festival (2) - second priority  
    elif date_str in festival_dates:
        day_type = 2
        day_category = "Festival"
    # Priority 3: Normal day including weekends (1) - lowest priority
    else:
        day_type = 1
        day_category = "Normal/Weekend"
    
    # Additional info
    weekday = date_obj.strftime('%A')
    is_weekend = date_obj.weekday() >= 5  # Saturday=5, Sunday=6
    
    day_type_data.append({
        'date': date_str,
        'day_type': day_type,
        'day_category': day_category,
        'weekday': weekday,
        'is_weekend': is_weekend,
        'day_of_month': date_obj.day,
        'month': date_obj.month,
        'year': date_obj.year
    })

# Create DataFrame
day_type_df = pd.DataFrame(day_type_data)

print(f"\nDay type mapping for September 2025:")
print(day_type_df)

# Summary statistics
print(f"\nSummary:")
day_type_counts = day_type_df['day_category'].value_counts()
for category, count in day_type_counts.items():
    print(f"  {category}: {count} days")

# Save to CSV
output_file = "day_type_mapping_sep2025.csv"
day_type_df.to_csv(output_file, index=False)
print(f"\nDay type mapping saved to: {output_file}")

# Show specific examples
print(f"\nExamples:")
for day_type in [0, 1, 2]:
    examples = day_type_df[day_type_df['day_type'] == day_type].head(3)
    if len(examples) > 0:
        type_name = examples.iloc[0]['day_category']
        print(f"  {type_name} (type {day_type}):")
        for _, row in examples.iterrows():
            print(f"    {row['date']} ({row['weekday']})")

print(f"\nMapping completed successfully!")