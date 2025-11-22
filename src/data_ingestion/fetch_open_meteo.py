# -*- coding: utf-8 -*-
"""
Created on Fri Nov 14 21:03:15 2025

@author: elakkiya
"""
import os
import pandas as pd
from datetime import datetime
from prefect import flow, task, get_run_logger
from open_meteo_predefined_functions import (
    get_open_meteo_data,
    get_incremental_dates,
    map_weather_code,
    feature_engineering,
    latitude,
    longitude,
    DAILY_VARIABLES,
    TIMEZONE,
)

#%%

DATA_DIR = "data/raw/open_meteo"
MASTER_PATH = os.path.join(DATA_DIR, "master_open_meteo.parquet")

#%%
#task1 here executes our get_open_meteo_data from open_meteo_predefined_functions.py
@task(retries=2, retry_delay_seconds=15)
def get_open_meteo_data_task():
    logger = get_run_logger()
    logger.info("Fetching Open Meteo data...")
    try:
        start_date, end_date = get_incremental_dates(MASTER_PATH)
        logger.info(f"Fetching data from {start_date} to {end_date}")
        df = get_open_meteo_data(lat=latitude, long=longitude,
                                 start_date = start_date, end_date = end_date, 
                                 vars = DAILY_VARIABLES,
                                 timezone = TIMEZONE)
        if df.empty:
            logger.warning("No new rows fetched (incremental fetch returned 0 rows)")
        else:
            logger.info(f"Fetched {len(df)} rows till {end_date} from Open Meteo...")
        return df
    except Exception as e:
        logger.error(f"Data fetch from Open Meteo Failed: {e}")
        raise

#task2 here maps our weather code to weather name
@task(retries=2, retry_delay_seconds= 5)
def map_weather_code_task(df):
    logger = get_run_logger()
    logger.info("Mapping Weather codes...")
    try:
        mapped_df = map_weather_code(df)
        logger.info("Weather Mapping Done...")
        return mapped_df
    except Exception as e:
        logger.error(f"Weather Mapping failed: {e}")
        raise

#task3 for feature engineering
@task(retries = 1, retry_delay_seconds = 5)
def feature_engineering_task(df):
    logger = get_run_logger()
    try:
        logger.info("Engineering features...")
        engg_df = feature_engineering(df)
        logger.info("Feature Engineering Done...")
        return engg_df
    except Exception as e:
        logger.error(f"Feature Engineering failed: {e}")
        raise
        
#task4 to save weather data
#exact dataset generated in one single Prefect run today, saved with a timestamp.
@task(retries=1, retry_delay_seconds=5)
def save_snapshot_task(df):
    logger = get_run_logger()
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        snapshot_path = os.path.join(DATA_DIR, f"snapshot_{timestamp}.parquet")
        df.to_parquet(snapshot_path, index=False)
        logger.info(f"Snapshot saved: {snapshot_path}")
        return snapshot_path
    except Exception as e:
        logger.error(f"Saving save failed: {e}")
        raise
#always-up-to-date final dataset
@task(retries=2, retry_delay_seconds=5)
def update_master_task(df):
    logger = get_run_logger()
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        #trying to read the parquet file
        if os.path.exists(MASTER_PATH):#to check whether we have this master path
            master_df = pd.read_parquet(MASTER_PATH)#read from the master path 
            combined = pd.concat([master_df, df], ignore_index=True)
            combined['date'] = pd.to_datetime(combined['date'])
            combined = (combined.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True))
            combined.to_parquet(MASTER_PATH, index=False)
            logger.info("Master file updated")
        else:#else sending the df directly to MASTER 
            df.to_parquet(MASTER_PATH, index=False)
            logger.info("Master file created...")
        return MASTER_PATH
    except Exception as e:
        logger.error(f"Master update failed... {e}")
        raise
    
#@flow runs our tasks
@flow(name="fetching_open_meteo_data_flow")
def fetch_open_meteo_data_flow():
    weather_df = get_open_meteo_data_task() #our task to get open meteo data between start date and end date
    save_snapshot_task(weather_df)
    update_master_task(weather_df)
    #weather_df = feature_engineering_task(weather_df)
    return weather_df

if __name__ =="__main__":
    
    result = fetch_open_meteo_data_flow()
    print(result.head())
#%%
"""
fetch_open_meteo_data_flow.deploy(
    name = 'open-meteo-weekly-refresh',
    work_pool_name = 'default-agent-pool',
    work_queue_name='weather-queue',
    schedule = {"cron":"0 5 * * 3",
                "timezone":"Asia/Kolkata"
                }
    )
"""