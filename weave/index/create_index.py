"""Home of the functionality concerning creating an index from a given file
system.
"""

import json
import os
import warnings

import pandas as pd

from ..config import get_index_column_names
from .list_baskets import _get_list_of_basket_jsons
from .validate_basket import validate_basket_dict


def create_index_from_fs(root_dir, file_system):
    """Recursively parse a pantry and create an index.

    Parameters
    ----------
    root_dir: str
        path to pantry
    file_system: fsspec object
        the fsspec file system hosting the bucket to be indexed.

    Returns
    ----------
    index: a pandas DataFrame with columns
           ["uuid", "upload_time", "parent_uuids",
            "basket_type", "label", "address", "storage_type"]
           and where each row corresponds to a single basket_manifest.json
           found recursively under specified root_dir.
    """

    # Check parameter data types
    if not isinstance(root_dir, str):
        raise TypeError(f"'root_dir' must be a string: '{root_dir}'")

    if not file_system.exists(root_dir):
        raise FileNotFoundError(f"'root_dir' does not exist '{root_dir}'")

    basket_jsons = _get_list_of_basket_jsons(root_dir, file_system)
    index_columns = get_index_column_names()
    index_dict = {}

    for key in index_columns:
        index_dict[key] = []

    bad_baskets = []
    for basket_json_address in basket_jsons:
        with file_system.open(basket_json_address, "rb") as file:
            basket_dict = json.load(file)
            if not validate_basket_dict(basket_dict):
                bad_baskets.append(os.path.dirname(basket_json_address))
                continue
            basket_dict["upload_time"] = pd.Timestamp(
                                                basket_dict["upload_time"]
                                         )
            if basket_dict["basket_type"] != "index":
                for field in basket_dict.keys():
                    index_dict[field].append(basket_dict[field])

                index_dict["address"].append(
                    os.path.relpath(os.path.dirname(basket_json_address))
                )
                index_dict["storage_type"].append(
                    file_system.__class__.__name__
                )

                if "weave_version" not in basket_dict.keys():
                    # Every basket uploaded before 0.13.0, should not have a
                    # version number, therefore every basket with no version
                    # number will be shown as <0.13.0
                    index_dict["weave_version"].append("<0.13.0")

    if len(bad_baskets) != 0:
        warnings.warn("baskets found in the following locations "
                      "do not follow specified weave schema:\n"
                      f"{bad_baskets}")

    index = pd.DataFrame(index_dict)
    index["uuid"] = index["uuid"].astype(str)
    return index
