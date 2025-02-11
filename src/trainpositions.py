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
         train_positions = data.get("TrainPositions", [])
            
         #make into df
         train_positions_df = pd.DataFrame(train_positions)
         #inner join to match location names
         location_db = conn.execute("Select * From Train_Stations s INNER JOIN Train_Circuits c on s.Station_Code = c.StationCode ").df()
         
         #Inner join to get station names
         df_merged = train_positions_df.merge(
        location_db, 
        left_on="CircuitId", 
        right_on="CircuitId", 
        how="inner"
    ).drop(columns=["StationCode"])
         
         df_merged = df_merged.dropna(subset=["Latitude"])
    except requests.exceptions.RequestException as e:
         print(f"Error occurred: {e}")   
    return df_merged  
