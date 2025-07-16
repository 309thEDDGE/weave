import os
import random
import json
from datetime import datetime, timedelta

def generate_dummy_baskets(basket_count=1000, file_count=10, file_size_mb=1, file_path="dummy_data"):
    """Generates dummy files in the specified directory with random text content.

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
    
    # Create the dummy files using the file count, size, and path provided
    for i in range(file_count):
        dir_path = os.path.join(file_path)
        os.makedirs(dir_path, exist_ok=True)

        # Create a dummy file of specified size
        size_in_bytes = file_size_mb * 1024 * 1024
        chunk_size = 1024
        
        # Generate data for the dummy file
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
    
    
