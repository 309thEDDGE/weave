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




def validate_bucket(bucket_name):    
    # print('validate_bucket_new: ', bucket_name)
    # fs = config.get_file_system()
    # basket_address = os.fspath(bucket_name)
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    if not s3fs_client.exists(bucket_name):
        raise ValueError(f"Invalid Bucket Path. Bucket does not exist at: {bucket_name}")
        return None
    
    return check_level(bucket_name) #call check level, with a path, but since we're just starting, we just use the bucket_name as the path
   
    
    



def check_level(current_dir_level):
    # print('check_level: ', current_dir_level)
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)

    manifest_path = os.path.join(current_dir_level, 'basket_manifest.json')
    # print('\t\t\tmanifest path: ', manifest_path)
    if s3fs_client.exists(manifest_path):
        #this is a bucket, because there is a manifest inside this dir
        # print('basket found at: ', current_dir_level)
        if validate_basket(current_dir_level):
            # print('valid basket at: ', current_dir_level)
            return True
        else:
            # print('invalid basket at: ', current_dir_level)
            return False
        
    else:
        # if there is no manifest at this level (no bucket here) then check all directories in this dir
        # if there is a dir, we call check_level() again at that level
        dirs_and_files = s3fs_client.find(path=current_dir_level, maxdepth=1, withdirs=True)
        # print('dirs and files:', dirs_and_files)
        
        for file_or_dir in dirs_and_files:
            file_type = s3fs_client.info(file_or_dir)['type']
            # print('file type: ', file_type)
            # print('file_or_dir:', file_or_dir)
            
            if file_type == 'directory':
                return check_level(file_or_dir)
    
    
    
'''  
    
    dirs_and_files = s3fs_client.find(path=current_dir_level, maxdepth=1, withdirs=True)
        
    for file_or_dir in dirs_and_files:
        file_type = s3fs_client.info(file_or_dir)['type']
        
        if file_type == 'file':
            # print('this is a file, check if its a manifest, if so, validate it')
            
            if is_basket(file_or_dir):
                # print('this is a basket, validate it')
                if not validate_basket(file_or_dir):
                    print('\n\tthis is not a valid basket')
                else:
                    print('this is a valid basket')
            # else:
            #     print('this is not a basket, probs just a regular file: ', file_or_dir)
                
        elif file_type == 'directory':
            # print('this is a dir, and we need to check the files at this level')
            check_level(file_or_dir)
        
'''
        
            
            
            
#     def print_file_list(files):
#         for file in files:
#             print('\n')
#             print("file:", file)
#             info = s3fs_client.info(file)
#             print('info on file: ', info)
            
#             if info['type'] == 'file':
#                 print('this is a file')
                
#             elif info['type'] == 'directory':
#                 print('this is a dir')
    
    
'''
def is_basket(directory):
    # print('is_basket: ', directory)
    
    basket_dir, bottom_file = os.path.split(directory)
    # print('head:', basket_dir)
    # print('tail:', bottom_file)
    
    # print('bottom file in is_basket: ', bottom_file)
    
    if bottom_file == 'basket_manifest.json':
        return True
        # print('we found a basket, now validate it')
        # print('basket_dir in is_bakset:', basket_dir)
        # print('directory: ', directory)
        # return validate_basket(basket_dir)
#             print('this is a valid basket')
            
#         else:
#             print('this is not a valid basket')
 
    
    return False
'''


def validate_basket(basket_dir):
    # this needs to validate the schema of the found manifest.
    # it also needs to see if a supplement exists, if so, validate it against schema. if not, invalid basket
    # it also needs to see if a metadata exists, if so, make sure it can be loaded into a json object.
    # print('validate_basket: ', basket_dir)
    
    # print('we are in validate_basket')
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')
    # print('supp path:', supplement_path)
    
    if not s3fs_client.exists(supplement_path):
        raise FileNotFoundError(f"Invalid Basket. No Supplement file found at: {supplement_path}")
    
    files_in_basket = s3fs_client.find(path=basket_dir, maxdepth=1, withdirs=True)
    
    
    for file in files_in_basket:
        
        basket_dir, file_name = os.path.split(file)
        
        if file_name == 'basket_manifest.json':
            try:
                data = json.load(s3fs_client.open(file))
                validate(instance=data, schema=config.manifest_schema)
                
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(f"Invalid Basket. Manifest Schema does not match at: {file}")
                return False
            
            except json.decoder.JSONDecodeError as err:
                raise ValueError(f"Invalid Basket. Manifest could not be loaded into json at: {file}")
                return False    
            
            
        if file_name == 'basket_supplement.json':
            try:
                data = json.load(s3fs_client.open(file))
                validate(instance=data, schema=config.supplement_schema)
                
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(f"Invalid Basket. Supplement Schema does not match at: {file}")
                return False
            
            except json.decoder.JSONDecodeError as err:
                raise ValueError(f"Invalid Basket. Supplement could not be loaded into json at: {file}")
                return False   
            

        if file_name == 'basket_metadata.json':
            try:
                data = json.load(s3fs_client.open(file))
                
            except json.decoder.JSONDecodeError as err:
                raise ValueError(f"Invalid Basket. Metadata could not be loaded into json at: {file}")
                return False 

            
        if s3fs_client.info(file)['type'] == 'directory':
            if check_level(file):
                raise ValueError(f"Invalid Basket. Manifest File found in sub directory of basket at: {basket_dir}")
            else:
                return True
            
            
    
    
    return True
    
    """
    

    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    files_in_basket = s3fs_client.find(path=basket_path, maxdepth=1, withdirs=True)
    # print('\n\n files in the found basket: ', files_in_basket)
    # print('\n')
    
    # print('basket path: ', basket_path)
    supplement_path = os.path.join(basket_path, 'basket_supplement.json')
    # print('supplement path: ', supplement_path)
    
    # print('basket_path: ', basket_path)
    # print('basket_join: ', os.path.join(basket_path, 'basket_supplement.json'))
    if not s3fs_client.exists(supplement_path):
        print('there was no supplement found here')
        raise FileNotFoundError(f"Invalid Basket, No Supplement file found at: {supplement_path}")
    else:
        print('there was a supplement found')
    
    man_schema = config.manifest_schema
    sup_schema = config.supplement_schema
    
    for file in files_in_basket:
        # print('current file working on: ', file)
        
        basket_dir, file_name = os.path.split(file)
        # print('head:', basket_dir)
        # print('tail:', bottom_file)
        # print('file_name:', file_name)
        
        # meta_schema = config.meta_schema
        
        if file_name == 'basket_manifest.json':
            
            data = json.load(s3fs_client.open(file))

            try:
                validate(instance=data, schema=man_schema)
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(f"Invalid Basket, Manifest Schema does not match at: {file}")
                return False
            # print('this is valid schema')    
            
            
        if file_name == 'basket_supplement.json':
            
            data = json.load(s3fs_client.open(file))

            try:
                validate(instance=data, schema=sup_schema)
            except jsonschema.exceptions.ValidationError as err:
                raise ValueError(f"Invalid Basket, Supplement Schema does not match at: {file}")
                return False
            # print('this is valid schema')    
            

        if file_name == 'basket_supplement.json':
            try:
                data = json.load(s3fs_client.open(file))
                # print('data:', data)
            except Exception as err:
                 raise ValueError(f"Invalid Basket, Metadata could not be loaded at: {file}")
            
        if s3fs_client.info(file)['type'] == 'directory':
            bask_in_bask = check_level(file)
            print('\n\nbask_in_bask: ', bask_in_bask)
    
    
    return True


"""




























'''




def isValid(data, schema):
    try:
        validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True


def validate_baskets(s3_client, path):
    
    
    
    head, tail = os.path.split(path)
    print(f"head path: {head}. Head info: {s3_client.info(head)}")
    print('tail: ', tail)
    head2, tail2 = os.path.split(head)
    print('head2: ', head2)
    print('tail2: ', tail2)
    
    
    
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
    
    # print('\n bucket_name:', bucket_name)
    
    fs = config.get_file_system()
    basket_address = os.fspath(bucket_name)
    
    # print('\n basket_address:', basket_address)
    
    
    ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
    s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
    if not s3fs_client.exists(bucket_name):
        raise ValueError(f"Invalid Bucket Path, Bucket does not exist at: {bucket_name}")
        return None
    
    #testing with minio and buckets
    # index = create_index_from_s3(bucket_name)
    # print('index:', index)
    
    # print('test before')
    # print('bucket_name:', bucket_name)
    
    # when you have the max depth=1, you get only the files and directories under that specific folder or path you give it.
    # using this, we can recursively go through every single file/directory path and see if it's a basket, if it
    # is a basket, then 
    file_list = s3fs_client.find(path=bucket_name, maxdepth=1, withdirs=True)
    
    filelist2 = s3fs_client.find(path=file_list[0], maxdepth=1, withdirs=True)
    
    filelist3 = s3fs_client.find(path=filelist2[0], maxdepth=1, withdirs=True)
    
    def print_file_list(files):
        for file in files:
            print('\n')
            print("file:", file)
            info = s3fs_client.info(file)['type']
            print('info on file: ', info)
            if info == 'file':
                print('this is a file')
            elif info == 'directory':
                print('this is a dir')

    
    
    print_file_list(file_list)
    print_file_list(filelist2)
    print_file_list(filelist3)
    
    
    # print('\n\nfile list 1: \n', file_list)
    # print('\n\nfile list 2: \n', filelist2)
    # print('\n\nfile list 3: \n', filelist3)
    
#     print('file list: ', file_list)
    
#     print('test after')
    
    # print('info: \n', s3fs_client.info(bucket_name))

        # validate_baskets(s3fs_client, file)
        
        
        
    

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
   
    
    
    
    
'''