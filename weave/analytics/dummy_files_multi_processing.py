import os
import shutil
import random
import json
import multiprocessing
from datetime import datetime, timedelta
from weave.basket import Basket
from weave.index.index_sqlite import IndexSQLite
from fsspec.implementations.local import LocalFileSystem

def generate_dummy_files_multi_processing(file_count=1000, file_size_mb=1, file_path="weave/analytics/dummy_multi_processing", num_processes=5):
    """Generates dummy files in the specified directory with random text content.

        Parameters:
        -----------
        file_path: str
            Defaults to "weave/analytics".  Specifies the directory where dummy files will be created.
        file_count: int
            Defaults to 1000.
            Specifies the number of dummy files to create.
        file_size_mb: int
            Defaults to 1.
            Specifies the size of each dummy file in megabytes. 
    """
    metadata_list = list()

    # Create the directories
    for i in range(file_count):
        dir_path = os.path.join(file_path, f"dummy_folder_{i}")
        os.makedirs(dir_path, exist_ok=True)

        # Create a dummy file of specified size
        size_in_bytes = file_size_mb * 1024 * 1024
        chunk_size = 1024
        
        # Generate random text and write to the file
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
                
                metadata_list.append(metadata)
                
                json.dump(metadata, f)
                
    return {"file_count":file_count, "file_size":file_size_mb, "file_location":file_path, "dummy metadata":metadata_list}
    
# Run if the script is executed directly
if __name__ == "__main__":
    
    # Specify the total files to generate and the number of processes
    total_files = 1000
    num_processes = 5
    
    #specify the number of files each process will handle
    files_per_process = total_files // num_processes
    
    #List to hold the process objects
    processes = []

    #Loop through the number of processes and create a process for each
    #Specify the path for each process
    #Append each process to the processes list
    #Start each process
    for i in range(num_processes):
        process_path = f"weave/analytics/dummy_multi_processing/process_{i}"
        p = multiprocessing.Process(
            target=generate_dummy_files_multi_processing,
            args=(files_per_process, 1, process_path, 1)
        )
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    print("✅ All 1000 files generated across 5 processes.")

    # Clean up the created directories and files
    if os.path.exists("weave/analytics/dummy_multi_processing"):
        shutil.rmtree("weave/analytics/dummy_multi_processing")
    print("✅ All files generated using 5 processes.")
    