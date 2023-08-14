"""
Home of the functionality concerning creating an index from a given file
system.
"""
import json
import os
import warnings

import pandas as pd

from ..config import index_schema
from .list_baskets import _get_list_of_basket_jsons
from .validate_basket import validate_basket_dict


def create_index_from_fs(root_dir, file_system):
    """Recursively parse an bucket and create an index

    Parameters:
        root_dir: str
            path to bucket
        file_system: fsspec object
            the fsspec file system hosting the bucket to be indexed.

    Returns:
        index: a pandas DataFrame with columns
               ["uuid", "upload_time", "parent_uuids",
                "basket_type", "label", "address", "storage_type"]
               and where each row corresponds to a single basket_manifest.json
               found recursively under specified root_dir
    """
    # check parameter data types
    if not isinstance(root_dir, str):
        raise TypeError(f"'root_dir' must be a string: '{root_dir}'")

    if not file_system.exists(root_dir):
        raise FileNotFoundError(f"'root_dir' does not exist '{root_dir}'")

    basket_jsons = _get_list_of_basket_jsons(root_dir, file_system)

    schema = index_schema()

    index_dict = {}

    for key in schema:
        index_dict[key] = []
    index_dict["address"] = []
    index_dict["storage_type"] = []

    bad_baskets = []
    for basket_json_address in basket_jsons:
        with file_system.open(basket_json_address, "rb") as file:
            basket_dict = json.load(file)
            if validate_basket_dict(basket_dict):
                for field in basket_dict.keys():
                    index_dict[field].append(basket_dict[field])
                index_dict["address"].append(os.path.dirname(basket_json_address))
                index_dict["storage_type"].append(file_system.__class__.__name__)
            else:
                bad_baskets.append(os.path.dirname(basket_json_address))

    if len(bad_baskets) != 0:
        warnings.warn('baskets found in the following locations '
                      'do not follow specified weave schema:\n'
                      f'{bad_baskets}')

    index = pd.DataFrame(index_dict)
    index["uuid"] = index["uuid"].astype(str)
    return index