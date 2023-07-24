"""
USAGE:
python create_index.py <root_dir> 
    root_dir: the root directory of s3 you wish to build your index off of
"""
import json
import os
import tempfile
from time import time_ns
from warnings import warn

import pandas as pd

from weave import config, upload


# validate basket keys and value data types on read in
def validate_basket_dict(basket_dict, basket_address):
    """
    validate the basket_manifest.json has the correct structure

    Parameters:
        basket_dict: dictionary read in from basket_manifest.json in minio
        basket_address: basket in question. Passed here to create better error
                        message
    """

    schema = config.index_schema()

    if list(basket_dict.keys()) != schema:
        raise ValueError(
            f"basket found at {basket_address} has invalid schema"
        )

    # TODO: validate types for each key


def create_index_from_s3(root_dir):
    """Recursively parse an s3 bucket and create an index

    Parameters:
        root_dir: path to s3 bucket

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

    fs = config.get_file_system()

    if not fs.exists(root_dir):
        raise FileNotFoundError(f"'root_dir' does not exist '{root_dir}'")

    basket_jsons = _get_list_of_basket_jsons(root_dir)

    schema = config.index_schema()

    index_dict = {}

    for key in schema:
        index_dict[key] = []
    index_dict["address"] = []
    index_dict["storage_type"] = []

    for basket_json_address in basket_jsons:
        with fs.open(basket_json_address, "rb") as file:
            basket_dict = json.load(file)
            validate_basket_dict(basket_dict, basket_json_address)
            for field in basket_dict.keys():
                index_dict[field].append(basket_dict[field])
            index_dict["address"].append(os.path.dirname(basket_json_address))
            index_dict["storage_type"].append("s3")

    index = pd.DataFrame(index_dict)
    index["uuid"] = index["uuid"].astype(str)
    return index

def _get_list_of_basket_jsons(root_dir):
    fs = config.get_file_system()
    return [x for x in fs.find(root_dir) if x.endswith("basket_manifest.json")]


class Index():
    '''Facilitate user interaction with the index of a Weave data warehouse.'''

    def __init__(self, bucket_name="basket-data", sync=True):
        '''Initializes the Index class.

        Parameters
        ----------
        bucket_name: [string]
            Name of the bucket which the desired index is associated with.
        sync: [bool]
            Whether or not to check the index on disk to ensure this Index
            object stays current. If True, then some operations may take
            slightly longer, as they will check to see if the current Index
            object has the same information as the index on the disk. If False,
            then the Index object may be stale, but operations will perform
            at a higher speed.
        '''
        self.bucket_name = str(bucket_name)
        self.index_basket_dir_name = 'index' # AKA basket type
        self.index_basket_dir_path = os.path.join(
            self.bucket_name, self.index_basket_dir_name
        )
        self.sync = bool(sync)
        self.index_json_time = 0 # 0 is essentially same as None in this case
        self.index_df = None
        pass
    
    def sync_index(self):
        '''Gets index from latest index basket'''
        fs = config.get_file_system()
        index_paths = fs.glob(f"{self.index_basket_dir_path}/**/*-index.json")
        if len(index_paths) == 0:
            return self.generate_index()
        if len(index_paths) > 20:
            warn(f"The index basket count is {len(index_paths)}. " +
                 "Consider running weave.Index.clean_up_indices")
        latest_index_path = ""
        for path in index_paths:
            path_time = self._get_index_time_from_path(path)
            if path_time >= self.index_json_time:
                self.index_json_time = path_time
                latest_index_path = path
        self.index_df = pd.read_json(
            fs.open(latest_index_path), dtype = {'uuid': str}
        )

    def _get_index_time_from_path(self, path):
        '''Returns time as int from index_json path.'''
        path = str(path)
        return int(os.path.basename(path).replace("-index.json",""))

    def to_pandas_df(self):
        '''Returns the Pandas DataFrame representation of the index.'''
        if self.sync:
            if not self.is_index_current():
                self.sync_index()
        elif self.index_df is None:
            self.sync_index()
        return self.index_df

    def clean_up_indices(self, n=20):
        '''Deletes any index basket except the latest n index baskets.
        
        Parameters
        ----------
        n: [int]
            n is the number of latest index baskets to retain.
        '''
        n = int(n)
        fs = config.get_file_system()
        index_paths = fs.glob(f"{self.index_basket_dir_path}/**/*-index.json")
        if len(index_paths) <= n:
            return
        index_time_list = [self._get_index_time_from_path(i)
                           for i in index_paths]
        index_times_to_keep = sorted(index_time_list, reverse=True)[:n]
        for index_time in index_time_list:
            if index_time not in index_times_to_keep:
                try:
                    path = fs.glob(
                        f"{self.index_basket_dir_path}/**/" +
                        f"{index_time}-index.json"
                    )[0]
                    uuid = path.split(os.path.sep)[-2]
                    self.delete_basket(basket_uuid=uuid)
                except ValueError as e:
                    warn(e)

    def is_index_current(self):
        '''Checks to see if the index in memory is up to date with disk index.

        Returns True if index in memory is up to date, else False.
        '''
        fs = config.get_file_system()
        index_paths = fs.glob(f"{self.index_basket_dir_path}/**/*-index.json")
        if len(index_paths) == 0:
            return False
        index_times = [self._get_index_time_from_path(i)
                      for i in index_paths]
        return all([self.index_json_time >= i for i in index_times])

    def generate_index(self):
        '''Generates index and stores it in a basket'''
        index = create_index_from_s3(self.bucket_name)
        self._upload_index(index=index)

    def _upload_index(self, index):
        """Upload a new index"""
        with tempfile.TemporaryDirectory() as out:
            ns = time_ns()
            temp_json_path = os.path.join(out, f"{ns}-index.json")
            index.to_json(temp_json_path)
            upload(upload_items=[{'path':temp_json_path, 'stub':False}],
                   basket_type=self.index_basket_dir_name,
                   bucket_name=self.bucket_name)
        self.index_df = index
        self.index_json_time = ns

    def delete_basket(self, basket_uuid):
        '''Deletes basket of given UUID.

        Note that the given basket will not be deleted if the basket is listed
        as the parent uuid for any of the baskets in the index.

        Parameters:
        -----------
        basket_uuid: [int]
            The uuid of the basket to delete.
        '''
        fs = config.get_file_system()
        basket_uuid = str(basket_uuid)
        if self.index_df is None:
            self.sync_index()
        if basket_uuid not in self.index_df["uuid"].to_list():
            raise ValueError(
                f"The provided value for basket_uuid {basket_uuid} " +
                "does not exist."
            )
        parent_uuids = [
            j
            for i in self.index_df["parent_uuids"].to_list()
            for j  in i
        ]
        if basket_uuid in parent_uuids:
            raise ValueError(
                f"The provided value for basket_uuid {basket_uuid} " +
                "is listed as a parent UUID for another basket. Please " +
                "delete that basket before deleting it's parent basket."
            )
        else:
            adr = self.index_df[
                self.index_df["uuid"] == basket_uuid
            ]["address"]
            fs.rm(adr, recursive=True)

    def upload_basket(self, upload_items, basket_type, parent_ids=[],
                      metadata={}, label=""):
        """Upload a basket to the same pantry referenced by the Index

        Parameters
        ----------
        upload_items : [dict]
            List of python dictionaries with the following schema:
            {
                'path': path to the file or folder being uploaded (string),
                'stub': true/false (bool)
            }
            'path' can be a file or folder to be uploaded. Every filename
            and folder name must be unique. If 'stub' is set to True, integrity
            data will be included without uploading the actual file or folder.
            Stubs are useful when original file source information is desired
            without uploading the data itself. This is especially useful when
            dealing with large files.
        basket_type: str
            Type of basket being uploaded.
        parent_ids: optional [str]
            List of unique ids associated with the parent baskets
            used to derive the new basket being uploaded.
        metadata: optional dict,
            Python dictionary that will be written to metadata.json
            and stored in the basket in MinIO.
        label: optional str,
            Optional user friendly label associated with the basket.
        """
        self.sync_index()
        up_dir = upload(
            upload_items=upload_items,
            basket_type=basket_type,
            bucket_name=self.bucket_name,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
        )
        single_indice_index = create_index_from_s3(up_dir)
        self._upload_index(
            pd.concat([self.index_df, single_indice_index], ignore_index=True)
        )
