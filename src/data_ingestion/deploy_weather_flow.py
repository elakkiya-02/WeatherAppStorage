# -*- coding: utf-8 -*-
"""
Created on Sat Nov 15 20:48:29 2025

@author: elakkiya
"""
#%%
from prefect.deployments import Deployment
from fetch_open_meteo import fetch_open_meteo_data_flow
from prefect_github.repository import GitHubRepository
#%%

#loading our github storage block

github_repository_block = GitHubRepository.load("weather-storage")
#creating our deployment object
deployment = Deployment(flow = fetch_open_meteo_data_flow,
                        name="open-meteo-serverless",
                        storage=github_repository_block,
                        work_pool_name = "default", #serverless
                        tags = ["weather", "serverless"],
                        path = "src/data_ingestion",
                        entrypoint="fetch_open_meteo.py:fetch_open_meteo_data_flow",
                        )
#registering the deployment with Prefect Cloud
if __name__ == "__main__":
    deployment.apply()
    print("[INFO] serverless deployment created...")
#%%