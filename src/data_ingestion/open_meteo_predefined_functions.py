# -*- coding: utf-8 -*-
"""
Created on Fri Nov 14 21:03:54 2025

@author: elakkiya
"""
#%%IMPORT LIBRARIES
import os
import pandas as pd
import requests
from datetime import datetime, date, timedelta
import numpy as np
#%%VARIABLES
latitude=13.0845
longitude = 87.2705
START = "2015-01-01"#getting past 10 years of data
#END = '2025-11-13'
END = date.today().strftime("%Y-%m-%d")
DAILY_VARIABLES = "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,wind_speed_10m_max"
#DAILY VARIABLES based on the availability as per the API documentation
TIMEZONE = "Asia/Kolkata"
#refer https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
#%%GET OPEN METEO DATA through API call

def get_open_meteo_data(lat, long, start_date, end_date, vars, timezone):
    url = "https://archive-api.open-meteo.com/v1/archive"
    archive_params = {
        "latitude": lat,
        "longitude": long,
        "start_date": start_date,
        "end_date": end_date,
        "daily":vars,
        "timezone":timezone
        }
    response = requests.get(url, params=archive_params)
    response.raise_for_status()
    historical_data = response.json()
    daily_data = pd.DataFrame(historical_data['daily'])
    #print(daily_data.columns)
    daily_data["time"] = pd.to_datetime(daily_data['time'])
    
    daily_data = daily_data.rename(columns={"time" : "date",
                                             "weather_code" : "weather",
                                             "temperature_2m_max" : "temp_max",
                                             "temperature_2m_min" : "temp_min",
                                             "precipitation_sum" : "rainfall",
                                             "rain_sum" : "sum_of_daily_rain",
                                             "wind_speed_10m_max" : "wind_speed"})
    return daily_data
#%%WEATHER MAP 
weather_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light",
    53: "Drizzle: Moderate",
    55: "Drizzle: Dense intensity",
    56: "Freezing Drizzle: Light",
    57: "Freezing Drizzle: Dense intensity",
    61: "Rain: Slight",
    63: "Rain: Moderate",
    65: "Rain: Heavy intensity",
    66: "Freezing Rain: Light",
    67: "Freezing Rain: Heavy intensity",
    71: "Snow fall: Slight",
    73: "Snow fall: Moderate",
    75: "Snow fall: Heavy intensity",
    77: "Snow grains",
    80: "Rain showers: Slight",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    85: "Snow showers: Slight",
    86: "Snow showers: Heavy",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
}

def map_weather_code(df):
    #df['weather'] = [weather_map.get(w_code, str(w_code)) for w_code in df['weather']]
    df["weather"] = df["weather"].map(weather_map).fillna(df["weather"].astype(str))
    return df
#%%FEATURE ENGINEERING
def feature_engineering(df):
    df_here = df.copy()
    df_here["date"] = pd.to_datetime(df_here["date"])
    df_here['day_of_year'] = df_here['date'].dt.dayofyear
    df_here['month'] = df_here['date'].dt.month
    df_here['weekday'] = df_here['date'].dt.weekday
    #CYCLICAL ENCODING
    df_here['day_sin'] = np.sin(2*np.pi*df_here['day_of_year']/365)
    df_here['day_cos'] = np.cos(2*np.pi*df_here['day_of_year']/365)
    #LAG FEATURES
    for lag in [1,2,3,7,14]:
        df_here[f"temp_max_lag_{lag}"] = df_here['temp_max'].shift(lag)
        
    #ROLLING AVGERAGE
    df_here['rolling_avg7_temp'] = df_here["temp_max"].shift(1).rolling(7).mean()
    #TARGET
    df_here['temp_max_next_day'] = df_here['temp_max'].shift(-1)
    return df_here.dropna().reset_index(drop=True)

#%%FOR INCREMENTAL INGESTION
def get_incremental_dates(master_path):
    try:
        if os.path.exists(master_path):
            master_df = pd.read_parquet(master_path)
            master_df['date'] = pd.to_datetime(master_df['date'])
            last_date= master_df['date'].max()
            start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")#to start from the next date
        else:
            start_date = START
        end_date = date.today().strftime("%Y-%m-%d")
        return start_date, end_date
    except Exception as e:
        raise RuntimeError(f"Error determining incremental dates: {e}")
#%%       
            