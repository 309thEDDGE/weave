import os
import json
import jsonschema
from jsonschema import validate
from pathlib import Path

from weave import config
from weave.create_index import create_index_from_s3
from fsspec.implementations.local import LocalFileSystem
import s3fs


def validate_bucket(bucket_name): 
    """
    Starts the validation process off based off the name of the bucket
    
    Validates that the bucket actually exists at the location given.
    If there is a bucket that exists, check it and every subdirectory by 
    calling check_level
    
    Parameters
    ----------
    bucket_name: string
        the name of the bucket in s3fs
        
    Returns
    ----------
    A bool of whether the bucket is valid or not that comes from check_level()
    """
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    if not s3fs_client.exists(bucket_name):
        raise ValueError(
            f"Invalid Bucket Path. Bucket does not exist at: {bucket_name}"
        )
    # call check level, with a path, but since we're just starting, 
    # we just use the bucket_name as the path
    return check_level(bucket_name) 


def check_level(current_dir):    
    """
    Checks all the immediate subdirectories and files in the given directory 
    to see if there is a basket_manifest.json file. If there is a manifest, 
    it's a basket and must be validated.
    If there is a directory found, then recursively call check_level with that
    found directory to find if there is a basket in any directory
    
    Parameters
    ----------
    current_dir: string
        the current directory that we want to search all files and 
        directories of
    
    Returns
    ----------
    bool that comes from:
        a validate_basket() if there is a basket found
        a check_level() if there is a directory found
        a default true if no basket is found
    """
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)

    manifest_path = os.path.join(current_dir, 'basket_manifest.json')
    
    # if a manifest exists, its a basket, validate it
    if s3fs_client.exists(manifest_path):
        return validate_basket(current_dir)
    # go through all the other files, if it's a directory, we need to check it
    else:
        dirs_and_files = s3fs_client.find(
            path=current_dir, maxdepth=1, withdirs=True
        )
        
        for file_or_dir in dirs_and_files:
            file_type = s3fs_client.info(file_or_dir)['type']
            
            if file_type == 'directory':
                #we don't want to reutrn check_level here because 
                #if it's valid, then you still need to 
                # check the rest of the dirs
                if not check_level(file_or_dir): 
                    return False
    
    # This is a backup return because if there are no baskets at all,
    # and no other files, then return true, because the structure is still
    # valid, but we just didn't find a basket to validate
    return True


def validate_basket(basket_dir):   
    """
    Takes the root directory of a basket and validates it
    
    Validation means there is a required basket_manifest.json and
    basket_supplment.json. And an optional basket_metadata.json
    The manifest and supplement are required to follow a certain schema,
    which is defined in config.py.
    All three, manifest, supplement, and metadata are also verified by being 
    able to be read into a python dictionary
    
    If there are any directories found inside this basket, run check_basket()
    on them to see if there is another basket inside this basket. If there is
    another basket, raise error for invalid bucket.
    
    If the Basket is ever invalid, raise an error
    If the basket is valid return true
    
    Parameters
    ----------
    basket_dir: string
        the path in s3fs to the basket root directory
        
    Returns
    ----------
    boolean that is true when the Basket is valid
        if the Basket is invalid, raise an error
    """
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')
    
    # a valid basket has both manifest and supplement
    if not s3fs_client.exists(supplement_path):
        raise FileNotFoundError(
            f"Invalid Basket. No Supplement file found at: {supplement_path}"
        )
    
    files_in_basket = s3fs_client.find(
                                path=basket_dir, 
                                maxdepth=1, 
                                withdirs=True
                            )
        
    for file in files_in_basket:   
        basket_dir, file_name = os.path.split(file)
        
        if file_name == 'basket_manifest.json':
            try:
                # these two lines make sure it can be read and is valid schema
                data = json.load(s3fs_client.open(file))
                validate(instance=data, schema=config.manifest_schema)
                
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(
                    f"Invalid Basket. "
                    f"Manifest Schema does not match at: {file}"
                )
            
            except json.decoder.JSONDecodeError as err:
                raise ValueError(
                    f"Invalid Basket. "
                    f"Manifest could not be loaded into json at: {file}"
                )
            
            
        if file_name == 'basket_supplement.json':
            try:
                # these two lines make sure it can be read and is valid schema
                data = json.load(s3fs_client.open(file))
                validate(instance=data, schema=config.supplement_schema)
                
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(
                    f"Invalid Basket. "
                    f"Supplement Schema does not match at: {file}"
                )
            
            except json.decoder.JSONDecodeError as err:
                raise ValueError(
                    f"Invalid Basket. "
                    f"Supplement could not be loaded into json at: {file}"
                )
            

        if file_name == 'basket_metadata.json':
            try:
                data = json.load(s3fs_client.open(file))
                
            except json.decoder.JSONDecodeError as err:
                raise ValueError(
                    f"Invalid Basket. "
                    f"Metadata could not be loaded into json at: {file}"
                )

        # if we find a directory inside this basket, we need to check it
        # if we check it and find a basket, this basket is invalid.
        if s3fs_client.info(file)['type'] == 'directory':
            if check_level(file):
                raise ValueError(
                    f"Invalid Basket. "
                    f"Manifest File found in sub directory of "
                    f"basket at: {basket_dir}"
                )
                
    # default return true if we don't find any problems with this basket
    return True