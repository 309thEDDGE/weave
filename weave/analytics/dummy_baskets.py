import os
import random
import json
import uuid
from pathlib import Path
from datetime import datetime, timedelta

def generate_dummy_baskets(basket_count=1000, file_count=10, file_size_mb=1, 
    file_path="dummy_data", num_basket_types=5):
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
            
        Returns
        ---------
        a list of dictionaries, each representing a basket with dummy files.
    """
    
    #List of all baskets created
    basket_list = []
    
    #Set up the dummy directory
    dir_path = os.path.join(str(file_path))
    if file_count > 0:
        os.makedirs(dir_path, exist_ok=True)
    
    # Create the dummy files using the file count, size, and path provided
    if file_count > 0:
        for i in range(file_count):

            # Create a dummy file of specified size
            size_in_bytes = file_size_mb * 1024 * 1024
            chunk_size = 1024
            
            # Generate data for the dummy file
            with open(os.path.join(dir_path, f"dummy_file_{i}.txt"), "w") as f:
                for _ in range(int((size_in_bytes // chunk_size))):
                    airline = random.choice(['UA', 'DL', 'AA', 'SW'])
                    flight_num = random.randint(100, 9999)
                    flight_number = f"{airline}{flight_num}"
                    now = datetime.now()
                    departure_time = now.strftime("%Y-%m-%d %H:%M")
                    hours = random.randint(1, 12)
                    arrival_time = (
                        now + timedelta(hours=hours)
                    ).strftime("%Y-%m-%d %H:%M")
                    origin = random.choice([
                        'JFK', 'LAX', 'ORD', 'ATL', 'DFW'
                    ])
                    destination = random.choice([
                        'SEA', 'MIA', 'DEN', 'PHX', 'SFO'
                    ])
                    aircraft_type = random.choice([
                        'A320', 'B737', 'B777', 'A380'
                    ])
                    altitude = random.randint(30000, 41000)  # feet
                    speed = random.randint(400, 600)  # knots
                    status = random.choice([
                        'Scheduled', 'Departed', 'Arrived', 'Delayed'
                    ])

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
    counter = 0
    x = 1
    for _ in range(basket_count):
        basket_list.append({"upload_items": [{'path' :file_path, 'stub': False}
            ], "basket_type": f"dummy-baskets-{x}", 
            "metadata": {'Data Type': 'text'}})

        if(num_basket_types > 0):
            counter += 1
            if counter % int(basket_count / num_basket_types) == 0:
                x += 1

    # Return the basket list
    return basket_list
