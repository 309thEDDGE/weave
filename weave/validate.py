#Validate bucket
#basket_data
#take in the bucket name as the argument

import os
import json
from jsonschema import validate

from weave import config
from fsspec.implementations.local import LocalFileSystem
import s3fs



def validate_bucket(bucket_name):
    # the bucket name would be something like this: 
    # 7b71a3f0654093511babbeeba3d0242c0a82011
    # this is like a folder name, then there should be:
    # manifest, supplement, and sometimes the metadata. 
    # if we do have the metadata, we need to make sure we can read it into a json object
    # my current working notes too:
    # bucket_name/basket_type/unique_id
    
    basket_address = os.fspath(bucket_name)
    manifest_path = f"{basket_address}/basket_manifest.json"
    supplement_path = f"{basket_address}/basket_supplement.json"
    metadata_path = f"{basket_address}/basket_metadata.json"
    
    
    
    
    if os.path.isfile(manifest_path):
        print('manifest is a real file')
        f = open(manifest_path)
        
        data = json.load(f)
        # print(data)
        
        validate(instance=data, schema=config.manifest_schema)
        
    
    if os.path.isfile(supplement_path):
        print('supplement is a real file')        
        f = open(supplement_path)
        
        data = json.load(f)
        # print(data)
        
        validate(instance=data, schema=config.supplement_schema)
        
        
              
    if os.path.isfile(metadata_path):
        print('metadata is a real file')        
        f = open(metadata_path)
        
        data = json.load(f)
        # print(data)
        
        
        

        
tempPath = './TestingValidation/'
validate_bucket(tempPath)
   
    