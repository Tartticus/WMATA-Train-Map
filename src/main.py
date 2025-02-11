import duckdb
import folium
import pyproj
import pandas as pd
import requests
import os


train_directory = os.getenv("TRAIN_PROJECT_DIR")
os.chdir(train_directory)

from trainlocations2 import get_train_locations
from traindb import  update_train_stations_db, update_train_location_db, create_db_tables, delete_tables
from trainmap import create_train_map

# Replace with your WMATA API key
api_key = os.getenv("WMATA_API_KEY")



def main():
    db_dec = input("Would you like to restart database? (y/n?) (If first time, say yes):\n")
    if db_dec == "y":
        delete_tables()
        create_db_tables()
        update_train_stations_db(api_key)
        print("Station Data Updated")
    print("Updating Train DB")
    update_train_location_db(api_key)
    print("Getting Train Locations")
    train_loc_df = get_train_locations(api_key)
    print("Creating map")
    create_train_map(train_loc_df)


if __name__ == "__main__":
    main()
