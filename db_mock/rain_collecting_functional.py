import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Bangkok BTS/MRT/ARL stations and major transport hubs
bangkok_locations = [
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

url = "https://api.open-meteo.com/v1/forecast"

all_data = []

# Loop through each location
for location in bangkok_locations:
    print(f"\nCollecting data for {location['name']}...")
    
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "daily": "rain_sum",
        "timezone": "Asia/Bangkok",
        "start_date": "2025-09-04",
        "end_date": "2025-09-30",
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    
    # Process daily data
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

# Combine all data
combined_dataframe = pd.concat(all_data, ignore_index=True)

print(f"\nCombined data from {len(bangkok_locations)} Bangkok locations")
print(combined_dataframe)

# Also create a summary by date (average across all locations)
summary_df = combined_dataframe.groupby('date')['rain_sum'].agg(['mean', 'min', 'max', 'std']).reset_index()
summary_df.columns = ['date', 'avg_rain_sum', 'min_rain_sum', 'max_rain_sum', 'std_rain_sum']


# Find heaviest rain days
heavy_rain_days = summary_df[summary_df['avg_rain_sum'] > 35.0]
print(f"\nHeavy rain days (>35mm/24hr):")
if not heavy_rain_days.empty:
    print(heavy_rain_days[['date', 'avg_rain_sum', 'max_rain_sum']])
else:
    print("No heavy rain days in this period")

# Show districts with highest rain on heaviest days
if not heavy_rain_days.empty:
    heaviest_day = summary_df.loc[summary_df['max_rain_sum'].idxmax()]
    print(f"\nHeaviest rain day: {heaviest_day['date']} with max {heaviest_day['max_rain_sum']:.1f}mm")
    heaviest_day_data = combined_dataframe[combined_dataframe['date'] == heaviest_day['date']]
    heaviest_locations = heaviest_day_data.nlargest(5, 'rain_sum')[['location', 'rain_sum']]
    print("Top 5 districts with most rain on that day:")
    print(heaviest_locations)

print(f"\nDaily summary across Bangkok:")
# print(summary_df)
summary_df.to_csv("rain_data_bangkok_summary.csv", index=False)