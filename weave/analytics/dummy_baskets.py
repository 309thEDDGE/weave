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

def generate_dummy_baskets(basket_count=1000, file_count=10, file_size_mb=1, file_path="dummy_data"):
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
            Defaults to "weave/analytics/dummy_data".
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

                data = {
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
                                
                json.dump(data, f)
                
    # Add a basket containing the dummy files to the basket list
    for _ in range(basket_count):
        basket_list.append({"upload_items": [{'path' :file_path, 'stub': False}], "basket_type": "dummy-baskets", "metadata": {'Data Type': 'text'}})

    # Return the basket list
    return basket_list
    

def run_index_basket_upload_test(basket_list, index, pantry_path="dummy-pantry", **kwargs):
    """Runs an upload test for the index type and specified number of baskets
       and files. The toal time taken to upload all these baskets will be
       printed and returned.

        Parameters:
        -----------
        basket_list: [dict]
            The list of baskets returned by generate_dummy_baskets to be used
            to upload in the test.
        index: IndexABC
            The concrete implementation of an IndexABC. This is used to track
            the contents within the pantry.
        pantry_path: str (default="dummy-pantry")
            The path to the pantry where we want to upload our basket_list.
        **file_system: str (default=LocalFileSystem())
            The fsspec object which hosts the pantry we desire to index.
        **num_basket_uploads (default=len(basket_list))
            The number of baskets we want to upload for the test.
        
        Returns
        ----------
        The total time in seconds for all of the baskets to be uploaded.
    """
    file_system = kwargs.get("file_system", LocalFileSystem())
    num_basket_uploads = kwargs.get("num_basket_uploads", len(basket_list))

    pantry = Pantry(index, pantry_path=pantry_path, file_system=file_system)

    #Extract the number of baskets we want to upload
    upload_baskets = basket_list[:num_basket_uploads]

    start_time = time.time()

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in upload_baskets:
        pantry.upload_basket(**basket)

    end_time = time.time()
    total_upload_time = end_time - start_time
    print(f"Time taken to upload {num_basket_uploads} baskets: {total_upload_time} seconds.")

    return total_upload_time
