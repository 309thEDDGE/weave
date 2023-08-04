import warnings
import os
import json
import jsonschema
from jsonschema import validate
from weave import config


def validate_bucket(bucket_name): 
    """Starts the validation process off based off the name of the bucket
    
    Validates that the bucket actually exists at the location given.
    If there is a bucket that exists, check it and every subdirectory by 
    calling _check_level
    
    Parameters
    ----------
    bucket_name: string
        the name of the bucket in s3fs
        
    Returns
    ----------
    A string of whether the bucket is valid or not 
    that comes from _check_level()
    """
    s3fs_client = config.get_file_system()
    
    # valid_bucket is a boolean to describe whether the current bucket is 
    # valid or invalid according to warnings raised
    # invalid_paths_list collects all of the invalid paths in the bucket,
    # if there are any
    valid_bucket = True
    invalid_paths_list = []
    
    if not s3fs_client.exists(bucket_name):
        warnings.warn(
            f"Invalid Bucket Path. Bucket does not exist at: {bucket_name}\n")
        invalid_paths_list.append(bucket_name)
        print("Invalid Bucket")
        print(f"INVALID LOCATIONS: {[bucket_name]}")
        return "Invalid Bucket", [bucket_name]
        
    # call check level, with a path, but since we're just starting, 
    # we just use the bucket_name as the path
    # create an empty array where we can store all of our invalid
    # bucket paths
    validate_bucket_statement = _check_level(bucket_name, 
                                             valid_bucket, 
                                             invalid_paths_list) 
    
    print(validate_bucket_statement)
    if len(invalid_paths_list) >= 1:
        print(f"INVALID LOCATIONS: {invalid_paths_list}")
    
    return validate_bucket_statement, invalid_paths_list


def _check_level(current_dir, valid_bucket, 
                 invalid_paths_list, in_basket=False):    
    """Check all immediate subdirs in dir, check for manifest
    
    Checks all the immediate subdirectories and files in the given directory 
    to see if there is a basket_manifest.json file. If there is a manifest, 
    it's a basket and must be validated.
    If there is a directory found, then recursively call check_level with that
    found directory to find if there is a basket in any directory.
    
    Parameters
    ----------
    current_dir: string
        the current directory that we want to search all files and 
        directories of the bucket
    valid_bucket: bool
        keeps track of whether the bucket is still valid or not. Default is
        True but will be set to False if any warning is raised.
    invalid_paths_list: list
        list of all of the invalid bucket locations. New invalid bucket 
        locations found in the function will be appended to this list.
    in_basket: bool
        optional parameter. This is a flag to signify that we are in a basket
        and we are looking for a nested basket now.
    
    Returns
    ----------
    str that comes from:
        a _validate_basket() if there is a basket found
        a _check_level() if there is a directory found
        Valid Basket if we found a manifest while inside another basket
        a default Valid Basket if no basket is found
    """
    
    s3fs_client = config.get_file_system()
    
    if not s3fs_client.exists(current_dir):
        warnings.warn(
            f"Invalid Path. No file or directory found at: {current_dir}\n")
        invalid_paths_list.append(current_dir)
        valid_bucket = False
    
    manifest_path = os.path.join(current_dir, 'basket_manifest.json')
    
    # if a manifest exists, its a basket, validate it
    if s3fs_client.exists(manifest_path):
        # if we find another manifest inside a basket, we just need to say 
        # we found it, we don't need to validate the nested basket
        if in_basket:
            return "Valid Bucket"
        return _validate_basket(current_dir, valid_bucket, invalid_paths_list)
        
    # go through all the other files, if it's a directory, we need to check it
    dirs_and_files = s3fs_client.find(
        path=current_dir, 
        maxdepth=1, 
        withdirs=True
    )
    
    for file_or_dir in dirs_and_files:
        file_type = s3fs_client.info(file_or_dir)['type']

        if file_type == 'directory':
            # if we are in a basket, check everything under it, for a manifest
            # and return true, this will return true to the _validate_basket
            # and throw an error
            if in_basket:
                if _check_level(file_or_dir, valid_bucket, invalid_paths_list, 
                                in_basket=in_basket) == "Valid Bucket":
                    return "Valid Bucket"
                return "Invalid Bucket"
            # if we aren't in the basket, we want to check all files in our
            # current dir. If everything is valid, _check_level returns true
            # if it isn't valid, we go in and return false
            # we don't want to return _check_level because we want to keep
            # looking at all the sub-directories
            else:
                if _check_level(file_or_dir, valid_bucket, invalid_paths_list, 
                                in_basket=in_basket) == "Invalid Bucket":
                    return "Invalid Bucket"
    
    # This is the default backup return. 
    # If we are in a basket, it will be valid if we return false,
    # because we want to signify that we didn't find another basket
    # If we are not in a basket, we want to return true, because 
    # we didn't find a basket and it was valid to have no baskets
    # return not in_basket
    if not in_basket:
        return "Valid Bucket"
    return "Invalid Bucket"

def _validate_basket(basket_dir, valid_bucket, invalid_paths_list):   
    """Takes the root directory of a basket and validates it
    
    Validation means there is a required basket_manifest.json and
    basket_supplment.json. And an optional basket_metadata.json
    The manifest and supplement are required to follow a certain schema,
    which is defined in config.py.
    All three, manifest, supplement, and metadata are also verified by being 
    able to be read into a python dictionary
    
    If there are any directories found inside this basket, run check_basket()
    on them to see if there is another basket inside this basket. If there is
    another basket, raise warning for invalid bucket.
    
    If the Basket is ever invalid, raise a warning and change valid_bucket
    to False
    If the basket is valid return Valid Bucket
    
    Parameters
    ----------
    basket_dir: string
        the path in s3fs to the basket root directory
    valid_bucket: bool
        keeps track of whether the bucket is still valid or not. Default is
        True but will be set to False if any warning is raised.
    invalid_paths_list: list
        list of all of the invalid bucket locations. New invalid bucket 
        locations found in the function will be appended to this list.
        
    Returns
    ----------
    str that is Valid Bucket when the Basket is valid
        if the Basket is invalid, raise a warning
        and change valid_bucket to False
    """
    s3fs_client = config.get_file_system()
    
    manifest_path = os.path.join(basket_dir, 'basket_manifest.json')
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')
    
    # a valid basket has both manifest and supplement
    # if for some reason the manifest is gone, we get a wrong directory,
    # or this function is incorrectly called, 
    # we can say that this isn't a basket. 
    if not s3fs_client.exists(manifest_path):
        warnings.warn(
            f"Invalid Path. No Basket found at: {basket_dir}\n")
        invalid_paths_list.append(basket_dir)
        valid_bucket = False
    
    if not s3fs_client.exists(supplement_path):
        warnings.warn(
            f"Invalid Basket. No Supplement file found at: "
            f"{supplement_path}\n")
        invalid_paths_list.append(supplement_path)
        valid_bucket = False
    
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
                
            except jsonschema.exceptions.ValidationError:
                warnings.warn(
                    f"Invalid Basket. Manifest Schema does not match at: "
                    f"{file}\n")
                invalid_paths_list.append(file)
                valid_bucket = False
            
            except json.decoder.JSONDecodeError:
                warnings.warn(f"Invalid Basket. Manifest could not be "
                              f"loaded into json at: {file}\n")
                invalid_paths_list.append(file)
                valid_bucket = False
            
            
        if file_name == 'basket_supplement.json':
            try:
                # these two lines make sure it can be read and is valid schema
                data = json.load(s3fs_client.open(file))
                validate(instance=data, schema=config.supplement_schema)
                
            except jsonschema.exceptions.ValidationError:
                warnings.warn(
                    f"Invalid Basket. Supplement Schema does not match at: "
                    f"{file}\n")
                invalid_paths_list.append(file)
                valid_bucket = False
            
            except json.decoder.JSONDecodeError:
                warnings.warn(f"Invalid Basket. Supplement could not be "
                              f"loaded into json at: {file}\n")
                invalid_paths_list.append(file)
                valid_bucket = False
            

        if file_name == 'basket_metadata.json':
            try:
                data = json.load(s3fs_client.open(file))
                
            except json.decoder.JSONDecodeError:
                warnings.warn(f"Invalid Basket. Metadata could not be "
                              f"loaded into json at: {file}\n")
                invalid_paths_list.append(file)
                valid_bucket = False

        # if we find a directory inside this basket, we need to check it
        # if we check it and find a basket, this basket is invalid.
        if s3fs_client.info(file)['type'] == 'directory':
            if _check_level(file, valid_bucket, 
                            invalid_paths_list, 
                            in_basket=True) == "Valid Bucket":
                warnings.warn(f"Invalid Basket. Manifest File found in sub "
                              f"directory of basket at: {basket_dir}\n")
                invalid_paths_list.append(basket_dir)
                valid_bucket = False
                
    # default return Valid Bucket if we don't find any problems with this basket
    if valid_bucket:
        return "Valid Bucket"
    return "Invalid Bucket"