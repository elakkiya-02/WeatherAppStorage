# -*- coding: utf-8 -*-
"""
Created on Sat Nov 15 20:48:29 2025

@author: elakkiya
"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(ROOT_DIR, "src", "data_ingestion"))
#%%
from prefect.deployments import Deployment
from fetch_open_meteo import fetch_open_meteo_data_flow
#%%
#creating our deployment object
deployment = Deployment(name="open-meteo-deployment",
                        flow = fetch_open_meteo_data_flow,
                        )
#registering the deployment with Prefect Cloud
if __name__ == "__main__":
    deployment.apply()
    print("[INFO] Deployment created sucessfully...")
#%%