'''
USAGE:
python create_index.py <root_dir> 
    root_dir: the root directory of s3 you wish to build your index off of
'''

import json
import argparse
import os
import pandas as pd
from weave import config

#validate basket keys and value data types on read in
def validate_basket_dict(basket_dict, basket_address):
    """
    validate the basket_manifest.json has the correct structure
    
    Parameters:
        basket_dict: dictionary read in from basket_manifest.json in minio
        basket_address: basket in question. Passed here to create better error message
    """
    
    schema = config.index_schema()
    
    if list(basket_dict.keys()) != schema:
        raise ValueError(f'basket found at {basket_address} has invalid schema')

    #TODO: validate types for each key

def create_index_from_s3(root_dir):
    """
    Recursively parse an s3 bucket and create an index using basket_manifest.json found therein
    
    Parameters:
        root_dir: path to s3 bucket
        
    Returns:
        index: a pandas DataFrame with columns
                ["uuid", "upload_time", "parent_uuids", "basket_type", "label", "address", "storage_type"]
                and where each row corresponds to a single basket_manifest.json
                found recursively under specified root_dir
    """
    
    #check parameter data types
    if not isinstance(root_dir, str):
        raise TypeError(f"'root_dir' must be a string: '{root_dir}'")
    
    fs = config.get_file_system()

    basket_jsons = [x for x in fs.find(root_dir) if x.endswith('basket_manifest.json')]

    schema = config.index_schema()
    
    index_dict = {}
    
    for key in schema:
        index_dict[key] = []
    index_dict['address'] = []
    index_dict['storage_type'] = []

    for basket_json_address in basket_jsons:
        with fs.open(basket_json_address, 'rb') as file:
            basket_dict = json.load(file)
            validate_basket_dict(basket_dict, basket_json_address)
            for field in basket_dict.keys():
                index_dict[field].append(basket_dict[field])
            index_dict['address'].append(os.path.dirname(basket_json_address))
            index_dict['storage_type'].append('s3')

    index = pd.DataFrame(index_dict)
    index['uuid'] = index['uuid'].astype(str)
    return index

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="""Save a local index of an s3 bucket built off of 
                        basket_details.json found within said bucket."""
    )
    argparser.add_argument(
        "root_dir",
        metavar="<root_dir>",
        type=str,
        help="the root directory of s3 you wish to build your index off of",
    )

    args = argparser.parse_args()

    create_index_from_s3(args.root_dir).to_parquet('index.parquet')
