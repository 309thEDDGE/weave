import json
import os
import tempfile
import pandas as pd
from weave import config
from weave import uploader

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

def create_index_from_s3(root_dir, fs):
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

    if not fs.exists(root_dir):
        raise FileNotFoundError(f"'root_dir' does not exist '{root_dir}'")

    basket_jsons = [
        x for x in fs.find(root_dir) if x.endswith("basket_manifest.json")
    ]

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

class Index():
    '''Facilitate user interaction with the index of a Weave data warehouse.'''

    def __init__(self, bucket_name):
        '''Initializes the Index class.

        Parameters
        ----------
        bucket_name: [string]
            Name of the bucket to be indexed.
        '''
        self.bucket_name = bucket_name
        self.index_dir = os.path.join(bucket_name, 'index')
        self.index_path = os.path.join(self.index_dir, 'index.json')
        self.fs = config.get_file_system()

        if not self.fs.exists(self.bucket_name):
            raise ValueError(
                f"Specified bucket does not exist: {self.bucket_name}"
            )

        if self.fs.exists(self.index_path):
            self.index_df = pd.read_json(
                self.fs.open(self.index_path),
                dtype = {'uuid': str}
            )
        else:
            self.index_df = None

    def update_index(self):
        '''Create a new index and upload it to the data warehouse.'''
        tempdir = tempfile.TemporaryDirectory()
        local_index_path = os.path.join(tempdir.name, 'index.json')

        try:
            #save remote index locally for posterity
            old_index_path = os.path.join(tempdir.name, 'old_index')
            os.mkdir(old_index_path)
            if self.fs.exists(self.index_path):
                self.fs.get(self.index_dir, old_index_path, recursive = True)
                self.fs.rm(self.index_dir, recursive = True)

            #create the index, and save it to a .json in the tempdir
            self.index_df = create_index_from_s3(self.bucket_name, self.fs)
            self.index_df.to_json(local_index_path)

            uploader.upload_basket(
                [{"path": local_index_path, "stub": False}],
                f"{self.bucket_name}/index",
                "0",
                "index",
            )

        except Exception as e:
            if not self.fs.exists(self.index_path):
                if os.path.exists(old_index_path):
                    self.fs.put(
                        old_index_path,
                        self.index_dir,
                        recursive = True
                    )
            raise e
        finally:
            tempdir.cleanup()

    def get_index(self):
        '''Return index_df. If it is None, then sync the index.'''
        if self.index_df is None:
            self.sync_index()

        return self.index_df

    def sync_index(self):
        '''Read in index json if it exists. if not, then create the index'''
        if self.fs.exists(self.index_path):
            self.index_df = pd.read_json(
                self.fs.open(self.index_path),
                dtype = {'uuid': str}
            )
        else:
            self.update_index()

