import folium
import pyproj
import pandas as pd


def create_train_map(train_loc_df):
    # Load the CSV file
    file_path = "Metro_Stations_Regional.csv"
    df = pd.read_csv(file_path)
    # Define the projection (assuming EPSG:3857 to EPSG:4326 conversion)
    transformer = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    
    
    
    # Convert X, Y to Latitude and Longitude
    df["Longitude"], df["Latitude"] = transformer.transform(df["X"], df["Y"])
    
    
    
    # Define color mapping for each metro line
    line_colors = {
        "red": "red",
        "blue": "blue",
        "green": "green",
        "yellow": "yellow",
        "orange": "orange",
        "silver": "gray",
    }
    
    # Create a base map centered around Washington, D.C.
    dc_map = folium.Map(location=[38.9072, -77.0369], zoom_start=11)
    
    
    
    # Add Metro station markers with color coding based on lines
    for _, row in df.iterrows():
        # Extract line color (default to black if unknown)
        line = row["LINE"].split(",")[0].strip().lower()  # Handling multiple lines
        color = line_colors.get(line, "black")
    
        folium.Marker(
            location=[row["Latitude"], row["Longitude"]],
            popup=f"{row['NAME']} ({row['LINE']})",
            icon=folium.Icon(color=color, icon="building", prefix="fa")
        ).add_to(dc_map)
    
    
    
    # Plot train positions on the map with pink markers
    for _, row in train_loc_df.iterrows():
        popup_data = f"""
    <b>Station:</b> {row['Station_Name']}<br>
    <b>Line:</b> {row['Line']}<br>
    <b>Train Number:</b> {row['TrainNumber']}
    """
        folium.Marker(
            location=[row["Latitude"], row["Longitude"]],
            popup=folium.Popup(popup_data, max_width=300),
            icon=folium.Icon(color="pink", icon="train", prefix="fa")  
        ).add_to(dc_map)
    
    # Save the map
    map_path = "dc_metro_map2.html"
    dc_map.save(map_path)
