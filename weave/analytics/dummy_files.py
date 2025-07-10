import os
import shutil
import string
import random
import time
import json
from datetime import datetime, timedelta
from weave.pantry import Pantry
from weave.basket import Basket
from weave.index.index_pandas import IndexPandas
from weave.index.index_sqlite import IndexSQLite
from fsspec.implementations.local import LocalFileSystem

def generate_dummy_files(basket_count=1000, file_count=10, file_size_mb=1, file_path="weave/analytics/dummy"):
    """Generates dummy files in the specified directory with random text content.

        Parameters:
        -----------
        basket_count: int
            Defaults to 1000.
            Specifies the number of dummy baskets to create.
        file_count: int
            Defaults to 10.
            Specifies the number of dummy files to create.
        file_size_mb: int
            Defaults to 1.
            Specifies the size of each dummy file in megabytes.
        file_path: str
            Defaults to "weave/analytics/dummy".
            Specifies the path where the dummy files will be created.
    """
    #List of all baskets created
    basket_list = []
    
    # Create the dummy files using the file count, size, and path provided
    for i in range(file_count):
        dir_path = os.path.join(file_path)
        os.makedirs(dir_path, exist_ok=True)

        # Create a dummy file of specified size
        size_in_bytes = file_size_mb * 1024 * 1024
        chunk_size = 1024
        
        # Generate metadata for the dummy file
        with open(os.path.join(dir_path, f"dummy_file_{i}.txt"), "w") as f:
            for _ in range(size_in_bytes // chunk_size):
                flight_number = f"{random.choice(['UA', 'DL', 'AA', 'SW'])}{random.randint(100,9999)}"
                now = datetime.now()
                departure_time = now.strftime("%Y-%m-%d %H:%M")
                arrival_time = (now + timedelta(hours=random.randint(1, 12))).strftime("%Y-%m-%d %H:%M")
                origin = random.choice(['JFK', 'LAX', 'ORD', 'ATL', 'DFW'])
                destination = random.choice(['SEA', 'MIA', 'DEN', 'PHX', 'SFO'])
                aircraft_type = random.choice(['A320', 'B737', 'B777', 'A380'])
                altitude = random.randint(30000, 41000)  # feet
                speed = random.randint(400, 600)  # knots
                status = random.choice(['Scheduled', 'Departed', 'Arrived', 'Delayed'])

                metadata = {
                    "flight_number": flight_number,
                    "departure_time": departure_time,
                    "arrival_time": arrival_time,
                    "origin": origin,
                    "destination": destination,
                    "aircraft_type": aircraft_type,
                    "altitude": altitude,
                    "speed": speed,
                    "status": status
                }
                                
                json.dump(metadata, f)
                
    # Add a basket containing the dummy files to the basket list
    for _ in range(basket_count):
        basket_list.append({"upload_items": [{'path' :file_path, 'stub': False}], "basket_type": "dummy-baskets", "metadata": {'Data Type': 'text'}})

    # Return the basket list
    return basket_list
    
