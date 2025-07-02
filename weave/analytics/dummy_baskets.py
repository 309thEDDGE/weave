import shutil
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from fsspec.implementations.local import LocalFileSystem

#Creates a dummy pantry to store baskets
def create_pantry(pantry_name):
    
    # Set up the file path
    local_fs = LocalFileSystem()
    folder_path = f"weave/analytics/{pantry_name}"

    #Set up the pantry with its uuid, manifest, and supplement
    pantry1 = Pantry(IndexPandas, pantry_path=folder_path, file_system=local_fs)
    
    #return the pantry
    return pantry1

#Generates 10 baskets with 1MB file in each
def generate_dummy_baskets(dummy_pantry, basket_name): 
    
    #Generate 1MB of data for the basket 
    with open("dummy_data.txt", 'w') as f:
        
        #each line is 100 bytes 10,000 lines = 1 MB
        for i in range(10000):
            f.write(f"Dummy metadata line {i:05d}: key=value; more=info; example=data\n")
    
    #Generate 10 baskets containing the 1MB file
    for uuid in range(0):
        uuid = str(uuid)
        dummy_pantry.upload_basket(upload_items=[{'path':'dummy_data.txt', 'stub':False}], basket_type="dummy_basket", metadata = {'Data Type':'text'})

#set the pantry and basket names
pantry_name = "dummy_pantry"
basket_name = "dummy_basket_1"
    
#generate dummy baskets in the pantry
generate_dummy_baskets(create_pantry(pantry_name), basket_name)

#remove the pantry - Uncomment the breakpoint to see the pantry with the baskets
#breakpoint()
shutil.rmtree("weave/analytics/dummy_pantry")
