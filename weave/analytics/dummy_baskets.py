""" 
This module provides utilities for generating and uploading dummy baskets
containing synthetic data files for testing and benchmarking the Weave pantry 
system.
"""
import os
import random
import time
import json
from datetime import datetime, timedelta

from weave.pantry import Pantry

def generate_dummy_baskets(basket_count=1000, file_count=10, file_size_mb=1,
    file_path="dummy_data", num_basket_types=1):
    """Generates dummy files in the specified directory with
    random text content. Each basket will contain the same list of
    dummy files from the dummy_data directory.

    Parameters:
    -----------
    basket_count: int (default=1000)
        Specifies the number of dummy baskets to create.
    file_count: int (default=10)
        Specifies the number of dummy files to create.
    file_size_mb: int (default=1)
        Specifies the size of each dummy file in megabytes.
    file_path: str (default="dummy_data")
        Specifies the path where the dummy files will be created.
    num_basket_types: int (default=1)
        Specifies the number of number of basket types to create.
    Returns
    ---------
    A list of dictionaries, each representing a basket with dummy files.
    """

    # We just want to return an empty list if no files are being created
    if file_count < 1:
        return []

    basket_list = []
    os.makedirs(file_path, exist_ok=True)
    size_in_bytes = file_size_mb * 1024 * 1024
    chunk_size = 1024
    for i in range(file_count):
        with open(os.path.join(file_path, f"dummy_file_{i}.txt"), "w",
                    encoding="utf-8") as f:
            for _ in range(int((size_in_bytes // chunk_size))):
                now = datetime.now()
                data = {
                    "flight_number": f"{random.choice(
                        ['UA', 'DL', 'AA', 'SW'])
                        }{random.randint(100, 9999)}",
                    "departure_time": now.strftime("%Y-%m-%d %H:%M"),
                    "arrival_time": (now + timedelta(
                        hours=random.randint(1, 12)))
                    .strftime("%Y-%m-%d %H:%M"),
                    "origin": random.choice(
                        ['JFK', 'LAX', 'ORD', 'ATL', 'DFW']),
                    "destination": random.choice(
                        ['SEA', 'MIA', 'DEN', 'PHX', 'SFO']),
                    "aircraft_type": random.choice(
                        ['A320', 'B737', 'B777', 'A380']),
                    "altitude": random.randint(30000, 41000),
                    "speed": random.randint(400, 600),
                    "status": random.choice(
                        ['Scheduled', 'Departed', 'Arrived', 'Delayed'])
                }
                json.dump(data, f)

    # We just want to return an empty list if no basket types are desired
    if num_basket_types < 1 or basket_count < 1:
        return []

    # Add a basket containing the dummy files to the basket list
    x = 1
    for i in range(basket_count):
        basket_list.append({
            "upload_items": [{'path': file_path, 'stub': False}],
            "basket_type": f"dummy-baskets-{x}",
            "metadata": {'Data Type': 'text'}
        })
        if num_basket_types > 0 and (i + 1) % int(
            basket_count / num_basket_types) == 0:
            x += 1
    return basket_list

def run_index_basket_upload_test(basket_list, pantry, **kwargs):
    """Runs an upload test for the index type and specified number of baskets
    and files. The toal time taken to upload all these baskets will be
    printed and returned.

    Parameters:
    -----------
    basket_list: [dict]
        The list of baskets returned by generate_dummy_baskets to be used
        to upload in the test.
    pantry: weave.Pantry
        Pantry object representing the pantry to upload baskets into.
    **num_basket_uploads (default=len(basket_list))
        The number of baskets we want to upload for the test.

    Returns
    ----------
    The total time in seconds for all of the baskets to be uploaded.
    """
    num_basket_uploads = kwargs.get("num_basket_uploads", len(basket_list))

    #Extract the number of baskets we want to upload
    upload_baskets = basket_list[:num_basket_uploads]

    start_time = time.time()

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in upload_baskets:
        pantry.upload_basket(**basket)

    end_time = time.time()
    total_upload_time = end_time - start_time

    return total_upload_time
