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


def validate_baskets(bucket_name):
    manifest_path = f"{basket_address}/basket_manifest.json"
    supplement_path = f"{basket_address}/basket_supplement.json"
    metadata_path = f"{basket_address}/basket_metadata.json"


def validate_bucket(bucket_name):
    
    fs = config.get_file_system()
    basket_address = os.fspath(bucket_name)

    # valide the bucket exists
    if not fs.exists(basket_address):
        raise ValueError(f"Invalid basket path: {basket_address}")
        return None
        
    
    manifest_path = f"{basket_address}/basket_manifest.json"
    supplement_path = f"{basket_address}/basket_supplement.json"
    metadata_path = f"{basket_address}/basket_metadata.json"
    
        
    output = {"manifest": False, "supplement": False, "metadata": False}
    
    
    if os.path.isfile(manifest_path):
        f = open(manifest_path)
        data = json.load(f)
        output['manifest'] = isValid(data, config.manifest_schema)
        
    else:
        raise FileNotFoundError(f"Invalid Basket, basket_manifest.json doest not exist at: {basket_address}")
        
    
    if os.path.isfile(supplement_path):        
        f = open(supplement_path)
        data = json.load(f)
        output['supplement'] =  isValid(data, config.supplement_schema)
        
    else:
        raise FileNotFoundError(f"Invalid Basket, basket_supplement.json doest not exist at: {basket_address}")
        
        
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
        

        
# tempPath = './TestingValidation/'
# tempPath = 'TERRIBLE'
# print("\n",validate_bucket(tempPath))
   
    