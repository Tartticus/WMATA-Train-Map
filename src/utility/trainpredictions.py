import requests
import folium
import pyproj
import pandas as pd
import duckdb



conn = duckdb.connect("train_data.db")








def get_train_locations(api_key):
    # API endpoint
    url = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
    
    
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
         location_data = response.json()  # Parse JSON response
         location_data2 = location_data['Trains']
         # Process and insert unique destinations with coordinates
         df_train_positions = pd.DataFrame([
    {
        "Location_Code": location.get("LocationCode"),
        "Location_Name": location.get("LocationName"),
        "Line_Color": location.get("Line")
    }
         for location in location_data2  # Ensure location_data2 is a list of dictionaries
         ])
         # Output some key information (modify as needed)
         print("Successfully retrieved train positions")
         # Extract TrainId and DestinationStationCode into a DataFrame
         
         
         location_db = conn.execute("Select * From Train_Stations").df()
         
         
         #Inner join to get station names
         df_merged = df_train_positions.merge(
        location_db, 
        left_on="Location_Code", 
        right_on="Station_Code", 
        how="inner"
    ).drop(columns=["Location_Code"])
         #Drop NAs
         df_merged = df_merged.dropna()
    except requests.exceptions.RequestException as e:
         print(f"Error occurred: {e}")   
    return df_merged      
