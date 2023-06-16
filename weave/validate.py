#Validate bucket
#basket_data
#take in the bucket name as the argument

import os
import json
import jsonschema
from jsonschema import validate
from pathlib import Path

from weave import config
from fsspec.implementations.local import LocalFileSystem
import s3fs



def isValid(data, schema):
    try:
        validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True


        


def validate_bucket(bucket_name):
    # the bucket name would be something like this: 
    # 7b71a3f0654093511babbeeba3d0242c0a82011
    # this is like a folder name, then there should be:
    # manifest, supplement, and sometimes the metadata. 
    # if we do have the metadata, we need to make sure we can read it into a json object
    # my current working notes too:
    # bucket_name/basket_type/unique_id
    
    fs = config.get_file_system()
    # print('this is the fs: ', fs)
    # print('fs tell: ', fs.tell())
    
    
    basket_address = os.fspath(bucket_name)

    #valide the bucket exists
    # if not fs.exists(basket_address):
    #     raise FileNotFoundError(f"Invalid Basket Path, cannot find file")
    
    
    manifest_path = f"{basket_address}/basket_manifest.json"
    supplement_path = f"{basket_address}/basket_supplement.json"
    metadata_path = f"{basket_address}/basket_metadata.json"
    
    
    output = {"manifest": False, "supplement": False, "metadata": False}
    
    
    if os.path.isfile(manifest_path):
        f = open(manifest_path)
        
        data = json.load(f)
        
        output['manifest'] = isValid(data, config.manifest_schema)
    else:
        output['manifest'] = 'No manifest found'
        
    
    if os.path.isfile(supplement_path):        
        f = open(supplement_path)
        
        data = json.load(f)
        
        output['supplement'] =  isValid(data, config.supplement_schema)
    else:
        output['supplement'] = 'No supplement found'
    
        
        
    if os.path.isfile(metadata_path):       
        try:
            f = open(metadata_path)
            data = json.load(f)
            output['metadata'] =  True
        except Exception as err:
            output['metadata'] =  False
            
    else:
        output['metadata'] = 'No metadata found'
            
    return output
        

        
tempPath = './TestingValidation/'
# tempPath = 'TERRIBLE'
print("\n",validate_bucket(tempPath))
   
    