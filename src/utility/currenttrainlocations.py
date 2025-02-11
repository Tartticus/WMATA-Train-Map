import requests
import folium
import pyproj
import pandas as pd
import duckdb



conn = duckdb.connect("train_data.db")








def get_current_train_locations(api_key):
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
    
        
         print("Successfully retrieved train positions")
         
         # Extract data from response 
         data2 = data.get("TrainPositions", [])
         
         df_train_positions = pd.DataFrame(data2)
         
    except requests.exceptions.RequestException as e:
         print(f"Error occurred: {e}")   
    return df_train_positions  
