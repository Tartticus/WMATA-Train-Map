# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import folium
import pyproj
import pandas as pd
import duckdb



conn = duckdb.connect("train_data.db")








def get_train_locations(api_key):
    # API endpoint
    url = "https://api.wmata.com/TrainPositions/TrainPositions"
    
    
    # Query parameters
    params = {
        "contentType": "json"
    }
    
    # Headers for the request
    headers = {
          "api_key": api_key
    }
    
           
    # Train Positions
    try:
         response = requests.get(url, headers=headers, params=params)
         response.raise_for_status()  # Raise an error for HTTP errors
         data = response.json()  # Parse JSON response
    
         # Output some key information (modify as needed)
         print("Successfully retrieved train positions")
         # Extract TrainId and DestinationStationCode into a DataFrame
         train_positions = [
            {"Train_ID": train.get("TrainId"), "Dest_Station": train.get("DestinationStationCode")}
            for train in data.get("TrainPositions", [])
        ]
    
         df_train_positions = pd.DataFrame(train_positions)
         location_db = conn.execute("Select * From Train_Stations").df()
         
         #Inner join to get station names
         df_merged = df_train_positions.merge(
        location_db, 
        left_on="Dest_Station", 
        right_on="Station_Code", 
        how="inner"
    ).drop(columns=["Dest_Station"])
    except requests.exceptions.RequestException as e:
         print(f"Error occurred: {e}")   
    return df_merged     
