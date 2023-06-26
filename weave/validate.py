#Validate bucket
#basket_data
#take in the bucket name as the argument


# psuedo code for how I'm going to take a bucket, and get all the jsons from it.
# when you get a bucket name, there could be any number of sub directories and baskets in it
# a sub directory could have even more sub directories in it with hundreds of baskets
# we know something is a basket because it will have a manifest, supplement, and sometimes a metadata
# 
# when given a Bucket name, it's very likely that there will be a set of folders directly in it.
# inside those folder, we could have more folders, or baskets.
# so, when given a Bucket name, go one folder deeper, check if there is a manifest.json
# if there is no manifest.json, try to go one folder deeper.
# if you go deeper, and there is still no manifest.json, try to go deeper.
# keep going deeper in the directories until you find a manifest.json.
# when we find a manifest.json, validate it and the supplement.json with it.

# Bucket name, check files inside
# check folders inside the Bucket for manifests
# if no manifests, go deeper
#  if there is a manifest, validate it with supplement and metadata
#  This is a Basket
# once Basket is validated, check other Baskets at the same level
# once all Baskets have been checked, make sure you go back out and have checked
# all the other directories in the Bucket
# 
# raise errors where needed, return true or something else to confirm Bucket is valid.  




import os
import json
import jsonschema
from jsonschema import validate
from pathlib import Path

from weave import config
from weave.create_index import create_index_from_s3
from fsspec.implementations.local import LocalFileSystem
import s3fs



def isValid(data, schema):
    try:
        validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True


def validate_baskets(s3_client, path):
    
    head, tail = os.path.split(path)
    # print('head: ', head)
    # print('tail: ', tail)
    
    
    
    if tail == "basket_manifest.json":
        # print('it is a manifest path: ', path)
        
        if s3_client.exists(path):
            f = s3_client.open(path)
            data = json.load(f)
            print('is manifest valid: ', isValid(data, config.manifest_schema))
            # output['manifest'] = isValid(data, config.manifest_schema)

        else:
            raise FileNotFoundError(f"Invalid Basket, basket_manifest.json doest not exist at: {path}")
        
        
        
        
        
        
    if tail == "basket_supplement.json":
        # print('it is a supplement')
        
        if s3_client.exists(path):        
            f = s3_client.open(path)
            data = json.load(f)
            # output['supplement'] =  isValid(data, config.supplement_schema)
            print('is supplement valid: ', isValid(data, config.supplement_schema))

        else:
            raise FileNotFoundError(f"Invalid Basket, basket_supplement.json doest not exist at: {path}")
       
        
        
        
    if tail == "basket_metadata.json":
        # print('it is a metadata')
        
        if s3_client.exists(path):       
            try:
                f = s3_client.open(path)
                data = json.load(f)
                print('is metadata valid: True')
                # output['metadata'] =  True
            except Exception as err:
                print('is metadata valid: False')
                # output['metadata'] =  False

        else:
            output['metadata'] = 'No metadata found'

    
    
    
    
    
    
    # manifest_path = f"{basket_address}/basket_manifest.json"
    # supplement_path = f"{basket_address}/basket_supplement.json"
    # metadata_path = f"{basket_address}/basket_metadata.json"


def validate_bucket(bucket_name):
    
    fs = config.get_file_system()
    basket_address = os.fspath(bucket_name)
    
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    if not s3fs_client.exists(bucket_name):
        raise ValueError(f"Invalid Bucket Path, it does not exist at: {bucket_name}")
        return None
    
    #testing with minio and buckets
    index = create_index_from_s3(bucket_name)
    print('index:', index)
    
    
    
    file_list = s3fs_client.find(bucket_name)
    
    for file in file_list:
        validate_baskets(s3fs_client, file)
        
        
        
    

    # valide the bucket exists
#     if not fs.exists(s3fs_client, basket_address):
#         raise ValueError(f"Invalid basket path: {basket_address}")
#         return None
        
    
#     manifest_path = f"{basket_address}/basket_manifest.json"
#     supplement_path = f"{basket_address}/basket_supplement.json"
#     metadata_path = f"{basket_address}/basket_metadata.json"
    
#     print('manifestpath: ', manifest_path)
#     print('supppath: ', supplement_path)
#     print('metapath: ', metadata_path)
    
        
    output = {"manifest": False, "supplement": False, "metadata": False}
    
    
#     if os.path.isfile(manifest_path):
#         f = open(manifest_path)
#         data = json.load(f)
#         output['manifest'] = isValid(data, config.manifest_schema)
        
#     else:
#         raise FileNotFoundError(f"Invalid Basket, basket_manifest.json doest not exist at: {basket_address}")
        
    
#     if os.path.isfile(supplement_path):        
#         f = open(supplement_path)
#         data = json.load(f)
#         output['supplement'] =  isValid(data, config.supplement_schema)
        
#     else:
#         raise FileNotFoundError(f"Invalid Basket, basket_supplement.json doest not exist at: {basket_address}")
        
        
#     if os.path.isfile(metadata_path):       
#         try:
#             f = open(metadata_path)
#             data = json.load(f)
#             output['metadata'] =  True
#         except Exception as err:
#             output['metadata'] =  False
            
#     else:
#         output['metadata'] = 'No metadata found'
            
    return output
        

        
# tempPath = './TestingValidation/'
# tempPath = 'TERRIBLE'
# print("\n",validate_bucket(tempPath))
   
    