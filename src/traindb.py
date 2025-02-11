#TrainLocation DB Setup
import duckdb
import folium
import pyproj
import pandas as pd
import requests
import datetime
from currenttrainlocations import get_current_train_locations

#SetUp DB
conn = duckdb.connect("train_data.db")


def delete_tables():
    try:
        conn.execute("Drop Table Train_Data")
        conn.execute("Drop Table Train_Stations")
        conn.execute("Drop Table Train_Circuits")
    except:
        pass
    print("Tables dropped")
    
def create_db_tables():
    # Create table if it doesn't exist
    conn.execute("""
    CREATE TABLE IF NOT EXISTS Train_Stations  (
        Station_Name TEXT PRIMARY KEY UNIQUE,
        Station_Code TEXT,
        Line TEXT,
        Latitude DOUBLE,
        Longitude DOUBLE
    );
    """)
    print("Station Table Created")
   # Create arrivals table if it doesn't exist
    conn.execute(""" CREATE TABLE IF NOT EXISTS Train_Data (
    Datetime DATETIME, 
    TrainId VARCHAR(10),
    TrainNumber VARCHAR(10),
    CarCount INT,
    DirectionNum INT,
    CircuitId INT,
    DestinationStationCode VARCHAR(10),
    LineCode VARCHAR(10),
    SecondsAtLocation INT,
    ServiceType VARCHAR(20)
    
);
    """)
    print("Locations Table Created")
    
    #Circuits table
    conn.execute(""" CREATE TABLE IF NOT EXISTS Train_Circuits (
    SeqNum INT,
    CircuitId INT,
    StationCode VARCHAR,
    Line TEXT,
    TrackNum INT,
    UNIQUE (SeqNum, CircuitId, Line, TrackNum)
    
);
    """)
    print("Circuits Table Created")
    
def update_train_stations_db(api_key):
    
    
    
    # Load the CSV file
    file_path = "Metro_Stations_Regional.csv"
    # API URL
    url = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
    
    
    
    # Query parameters
    params = {
        "contentType": "json"
    }
    
    # Headers for the request
    headers = {
          "api_key": api_key
    }
    
    
    df = pd.read_csv(file_path)
    # Define the projection (assuming EPSG:3857 to EPSG:4326 conversion)
    transformer = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    
    
    
    # Convert X, Y to Latitude and Longitude
    df["Longitude"], df["Latitude"] = transformer.transform(df["X"], df["Y"])
    
    # Load station coordinates from CSV into a dictionary for lookup
    station_coords = {
        row["NAME"]: (row["Latitude"], row["Longitude"]) for _, row in df.iterrows()
    }
    
    
    
    #### Get train locations for DB
    
    # Sending the GET request
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an error for HTTP errors
        location_data = response.json()  # Parse JSON response
        location_data2 = location_data['Trains']
        # Process and insert unique destinations with coordinates
        for location in location_data2:
            dest_code = location.get("LocationCode")
            dest_name = location.get("LocationName")
            colour = location.get("Line")
          
            if dest_code and dest_name:
                # Lookup coordinates from the CSV (default to None if not found)
                latitude, longitude = station_coords.get(dest_name, (None, None))
                if dest_name != "No Passenger":
                    try:
                        # Insert only if the destination is not already stored
                        conn.execute("""
                        INSERT INTO Train_Stations (Station_Name, Station_Code, Line, Latitude, Longitude)
                        VALUES (?, ?, ?, ?,?)
                        
                        """, (dest_name, dest_code, colour, latitude, longitude))
                        print(f"{dest_name} inserted")
                    except Exception as e:
                       
                       pass
    except Exception as e:
        print(e)
        pass



def update_train_location_db(api_key):
    
    from datetime import datetime
    #Get datetime for DB
    datetime  = datetime.now()
    

    df =  get_current_train_locations(api_key)
    
    df["Datetime"] = datetime
    
    #Convert datetime for duckdb
    
    # Convert to datetime format
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    
    # Convert to DuckDB format YYYY/MM/DD HH:MM:SS
    df["Datetime"] = df["Datetime"].dt.strftime("%Y/%m/%d %H:%M:%S")
    # move Datetime to column 1
    df = df[["Datetime"] + [col for col in df.columns if col != "Datetime"]]
    
    # Insert DataFrame into DuckDB
    conn.execute("INSERT INTO train_data SELECT * FROM df")
    print("Locations updated in db")


def update_train_circuits_db(api_key):
    #Update circuit info
    url = "https://api.wmata.com/TrainPositions/StandardRoutes?contentType=json"
    # Query parameters
    params = {
        "contentType": "json"
    }
    
    # Headers for the request
    headers = {
          "api_key": api_key
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raise an error for HTTP errors
    location_data = response.json()  # Parse JSON response
    location_data2 = location_data['StandardRoutes']
     
    #Turn List into table
    circuit_list = []
    for item in location_data2:
        if "TrackCircuits" in item:
            df = pd.DataFrame(item["TrackCircuits"])
            df['Line'] = item['LineCode']
            df['TrackNum'] = item['TrackNum']
            circuit_list.append(df)
    #concat df        
    circuit_df = pd.concat(circuit_list, ignore_index=True)
    
    conn.execute("INSERT INTO Train_Circuits SELECT * FROM circuit_df")         

