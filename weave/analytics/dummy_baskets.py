import os
import shutil
import string
import random
import time
from weave.pantry import Pantry
from weave.index.index_sqlite import IndexSQLite
from fsspec.implementations.local import LocalFileSystem


def generate_dummy_baskets(index_type, file_name="dummy_data.txt", basket_count=1000, print_time=True):
    """Generates a 1MB file of dummy data and uploads it to a pantry as 10 baskets.
    Used to test the time it takes to handle large files in the baskets.

    Parameters:
    -----------
    file_path: str
        Path to the dummy data file to be uploaded.
    index_type: Object
        Type of index to be used for the pantry.
    basket_count: int
        Number of baskets to be created (default is 1000).
    """
    #Create the 1MB random txt file
    size_in_bytes = 1024 * 1024
    chunk_size = 1024  # 1 KB chunks
    characters = string.ascii_letters + string.digits

    with open(file_name, "w") as f:
        for _ in range(size_in_bytes // chunk_size):
            random_text = ''.join(random.choices(characters, k=chunk_size))
            f.write(random_text)

    # Set up the file path
    local_fs = LocalFileSystem()
    
    #Set up the pantry with its index, path, uuid, manifest, and supplement
    dummy_pantry = Pantry(index_type, pantry_path="dummy_pantry", file_system=local_fs)
    
    if print_time:
        start_time = time.time()

    #Generate 10 baskets containing the 1MB file dummy_data.txt
    for uuid in range(basket_count):
        uuid = str(uuid)
        dummy_pantry.upload_basket(upload_items=[{'path':file_name, 'stub':False}], basket_type="dummy_baskets", metadata = {'Data Type':'text'})
    
    if print_time:
        end_time = time.time()
    
        time_elapsed = end_time - start_time
        print(f"Time taken to generate {basket_count} dummy baskets for {index_type.__name__}: {time_elapsed} seconds")

    #Cleanup the pantry
    shutil.rmtree(dummy_pantry.pantry_path)
    
    #Cleanup the .db file if using SQLite index
    if(index_type == IndexSQLite):
        dummy_pantry.index.drop_index()
    
    #Delete the dummy txt if it exists
    if os.path.exists(file_name):
        os.remove(file_name)
