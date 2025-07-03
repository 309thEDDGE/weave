import os
import shutil
from weave.pantry import Pantry
from weave.index.index_sqlite import IndexSQLite
from fsspec.implementations.local import LocalFileSystem

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
def generate_dummy_baskets(file_path, index_type, basket_count=1000): 
    
    # Set up the file path
    local_fs = LocalFileSystem()
    
    #Set up the pantry with its index, path, uuid, manifest, and supplement
    dummy_pantry = Pantry(index_type, pantry_path="dummy_pantry", file_system=local_fs)
    
    #Generate 10 baskets containing the 1MB file dummy_data.txt
    for uuid in range(basket_count):
        uuid = str(uuid)
        dummy_pantry.upload_basket(upload_items=[{'path':file_path, 'stub':False}], basket_type="dummy_baskets", metadata = {'Data Type':'text'})

    #Cleanup the pantry
    shutil.rmtree("dummy_pantry")
    
    #Cleanup the .db file if using SQLite index
    if(index_type == IndexSQLite):
        IndexSQLite.__del__(self=dummy_pantry.index)
        os.remove("weave-dummy_pantry.db")

