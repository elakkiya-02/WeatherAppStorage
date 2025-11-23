# -*- coding: utf-8 -*-
"""
Created on Fri Nov 14 21:03:15 2025

@author: elakkiya
"""
import os
import pandas as pd
import datetime
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
import io
import base64
from prefect_github.repository import GitHubRepository

#%%
GITHUB_DATA_DIR = "data/raw/open_meteo"
MASTER_FILENAME="master_open_meteo.parquet"
GITHUB_BLOCK_NAME = "weather-storage"
MASTER_REPO_PATH = os.path.join(GITHUB_DATA_DIR, MASTER_FILENAME)
#%%
def _github_block():
    #This will give us the methods to read and write files to our repo
    return GitHubRepository.load(GITHUB_BLOCK_NAME)

def read_parquet_from_github(path_in_repo: str) -> pd.DataFrame:
    logger = get_run_logger()
    github = _github_block()
    try:
        if hasattr(github, "read_path"):
            data_bytes = github.read_path(path_in_repo)
        elif hasattr(github, "read_bytes"):
            data_bytes = github.read_bytes(path_in_repo)
        elif hasattr(github, "get_file"):
            data_bytes = github.get_file(path_in_repo)
        else:
            raise AttributeError(
                "GitHubRepository block doesn't expose a known read method (read_path/read_bytes/get_file)"
                )
        if not data_bytes:
            logger.info(f"No bytes returned when reading {path_in_repo} from Github")
        if isinstance(data_bytes, dict) and "content" in data_bytes:
            content_b64 = data_bytes["content"]
            decoded_data = base64.b64decode(content_b64)
            buf = io.BytesIO(decoded_data)
        elif isinstance(data_bytes, bytes):
            buf = io.BytesIO(data_bytes)
        else:
            buf = io.BytesIO(bytes(data_bytes))
        
        df = pd.read_parquet(buf)
        return df
    except FileNotFoundError:
        logger.info(f"{path_in_repo} not found in GitHub storage, returning empty dataframe")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to read {path_in_repo} from GitHub: {e}")
        raise


def write_parquet_to_github(df: pd.DataFrame, path_in_repo: str):
    logger = get_run_logger()
    github = _github_block()
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    data_bytes = buf.read()
    try:
        if hasattr(github, "write_path"):
            github.write_path(path_in_repo, data_bytes)
        elif hasattr(github, "write_bytes"):
            github.write_bytes(path_in_repo, data_bytes)
        elif hasattr(github, "put_file"):
            github.put_file(path_in_repo, data_bytes)
        else:
            raise AttributeError(
                "GitHubRepository block doesn't expose a known write method (write_path/write_bytes/put_file)"
                )
        logger.info(f"Parquet Write to Github at {path_in_repo}")
    except Exception as e:
        logger.error(f"Failed to write parquet to Github at {path_in_repo}: {e}")
        raise
        
#%%
#task1 here executes our get_open_meteo_data from open_meteo_predefined_functions.py
@task(retries=2, retry_delay_seconds=15)
def get_open_meteo_data_task():
    logger = get_run_logger()
    logger.info("Fetching Open Meteo data...")
    
    try:
        master_df = read_parquet_from_github(MASTER_REPO_PATH)
        start_date, end_date = get_incremental_dates(master_df)
        logger.info(f"Fetching data from {start_date} to {end_date}")
        df = get_open_meteo_data(lat=latitude, long=longitude,
                                 start_date = start_date, 
                                 end_date =end_date, 
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
def save_snapshot_task(df: pd.DataFrame):
    logger = get_run_logger()
    try:
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        snapshot_name = f"snapshot_{timestamp}.parquet"
        snapshot_path= os.path.join(GITHUB_DATA_DIR, snapshot_name)
        write_parquet_to_github(df, snapshot_path)
        logger.info(f"Snapshot saved: {snapshot_path}")
        return snapshot_path
    except Exception as e:
        logger.error(f"Saving snapshot failed: {e}")
        raise
#always-up-to-date final dataset
@task(retries=2, retry_delay_seconds=5)
def update_master_task(new_df):
    logger = get_run_logger()
    try:
        master_df = read_parquet_from_github(MASTER_REPO_PATH)
        
        if master_df.empty:
            combined = new_df.copy()
        else:
            combined = pd.concat([master_df, new_df], ignore_index=True)
            
        if "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"])
            combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        
        write_parquet_to_github(combined, MASTER_REPO_PATH)
        logger.info("Master file created/updated in Github repository")
        return MASTER_REPO_PATH
    except Exception as e:
        logger.error(f"master update failed: {e}")
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
    print(result.head() if not result.empty else "No New rows fetched...")
    
#%%