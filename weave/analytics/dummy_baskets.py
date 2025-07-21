"""Functions for generating dummy baskets and files for testing."""

import os
import random
import json
from datetime import datetime, timedelta

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
    basket_list = []
    if file_count > 0:
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
