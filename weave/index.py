"""
USAGE:
python create_index.py <root_dir>
    root_dir: the root directory of s3 you wish to build your index off of
"""
import json
import os
import tempfile
import warnings
from time import time_ns

import pandas as pd
import jsonschema
from jsonschema import validate

import weave


# validate basket keys and value data types on read in
def validate_basket_dict(basket_dict):
    """validate the basket_manifest.json has the correct structure

    Parameters:
        basket_dict: dictionary read in from basket_manifest.json in minio

    Returns:
        valid (bool): True if basket has correct schema, false otherwise
    """

    try:
        validate(instance=basket_dict, schema=weave.config.manifest_schema)
        return True

    except jsonschema.exceptions.ValidationError:
        return False


def create_index_from_fs(root_dir, file_system):
    """Recursively parse an bucket and create an index

    Parameters:
        root_dir: path to bucket
        file_system: the fsspec file system hosting the bucket to be indexed.

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

    schema = weave.config.index_schema()

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


def _get_list_of_basket_jsons(root_dir, file_system):
    return [x
            for x
            in file_system.find(root_dir)
            if x.endswith("basket_manifest.json")]


class Index():
    '''Facilitate user interaction with the index of a Weave data warehouse.'''

    def __init__(self, bucket_name="basket-data", sync=True, **kwargs):
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

        kwargs:
        file_system: fsspec object
            The fsspec object which hosts the bucket we desire to index.
            If file_system is None, then the default fs is retrieved from the
            config.
        '''
        self.file_system = kwargs.get("file_system",
                                      weave.config.get_file_system())

        self.bucket_name = str(bucket_name)
        self.index_basket_dir_name = 'index' # AKA basket type
        self.index_basket_dir_path = os.path.join(
            self.bucket_name, self.index_basket_dir_name
        )
        self.sync = bool(sync)
        self.index_json_time = 0 # 0 is essentially same as None in this case
        self.index_df = None

    def sync_index(self):
        '''Gets index from latest index basket'''
        index_paths = self.file_system.glob(f"{self.index_basket_dir_path}"
                                            "/**/*-index.json")
        if len(index_paths) == 0:
            self.generate_index()
            return
        if len(index_paths) > 20:
            warnings.warn(f"The index basket count is {len(index_paths)}. " +
                 "Consider running weave.Index.clean_up_indices")
        latest_index_path = ""
        for path in index_paths:
            path_time = self._get_index_time_from_path(path)
            if path_time >= self.index_json_time:
                self.index_json_time = path_time
                latest_index_path = path
        self.index_df = pd.read_json(
            self.file_system.open(latest_index_path), dtype = {'uuid': str}
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

    def clean_up_indices(self, n_ret=20):
        '''Deletes any index basket except the latest n index baskets.

        Parameters
        ----------
        n_ret: [int]
            n is the number of latest index baskets to retain.
        '''
        n_ret = int(n_ret)
        index_paths = self.file_system.glob(f"{self.index_basket_dir_path}"
                                            "/**/*-index.json")
        if len(index_paths) <= n_ret:
            return
        index_time_list = [self._get_index_time_from_path(i)
                           for i in index_paths]
        index_times_to_keep = sorted(index_time_list,
                                     reverse=True)[:n_ret]
        for index_time in index_time_list:
            if index_time not in index_times_to_keep:
                try:
                    path = self.file_system.glob(
                        f"{self.index_basket_dir_path}/**/" +
                        f"{index_time}-index.json"
                    )[0]
                    uuid = path.split(os.path.sep)[-2]
                    self.delete_basket(basket_uuid=uuid, upload_index=False)
                except ValueError as error:
                    warnings.warn(error)

    def is_index_current(self):
        '''Checks to see if the index in memory is up to date with disk index.

        Returns True if index in memory is up to date, else False.
        '''
        index_paths = self.file_system.glob(f"{self.index_basket_dir_path}"
                                            "/**/*-index.json")
        if len(index_paths) == 0:
            return False
        index_times = [self._get_index_time_from_path(i)
                      for i in index_paths]
        return all((self.index_json_time >= i for i in index_times))

    def generate_index(self):
        '''Generates index and stores it in a basket'''
        index = create_index_from_fs(self.bucket_name, self.file_system)
        self._upload_index(index=index)

    def _upload_index(self, index):
        """Upload a new index"""
        with tempfile.TemporaryDirectory() as out:
            n_secs = time_ns()
            temp_json_path = os.path.join(out, f"{n_secs}-index.json")
            index.to_json(temp_json_path)
            weave.uploader_functions.UploadBasket(
                upload_items=[{'path':temp_json_path, 'stub':False}],
                basket_type=self.index_basket_dir_name,
                file_system=self.file_system,
                bucket_name=self.bucket_name
            )
        self.index_df = index
        self.index_json_time = n_secs

    def get_basket(self, basket_address):
        """Retrieves a basket of given UUID or path.

        Parameters
        ----------
        basket_address: string
            Argument can take one of two forms: either a path to the Basket
            directory, or the UUID of the basket.

        Returns
        ----------
        The Basket object associated with the given UUID or path.
        """
        # Create a Basket from the given address, and the index's file_system
        # and bucket name. Basket will catch invalid inputs and raise
        # appropriate errors.
        return weave.Basket(basket_address, self.bucket_name,
                            file_system=self.file_system)

    def delete_basket(self, basket_uuid, **kwargs):
        '''Deletes basket of given UUID.

        Note that the given basket will not be deleted if the basket is listed
        as the parent uuid for any of the baskets in the index.

        Parameters:
        -----------
        basket_uuid: int
            The uuid of the basket to delete.
        kwargs:
        upload_index: bool
            Flag to upload the new index to the file system
        '''
        upload_index = kwargs.get("upload_index", True)
        basket_uuid = str(basket_uuid)
        if self.index_df is None:
            self.sync_index()
        if basket_uuid not in self.index_df["uuid"].to_list():
            raise ValueError(
                f"The provided value for basket_uuid {basket_uuid} " +
                "does not exist."
            )
        # Flatten nested lists into a single list
        parent_uuids = [
            j
            for i in self.index_df["parent_uuids"].to_list()
            for j in i
        ]
        if basket_uuid in parent_uuids:
            raise ValueError(
                f"The provided value for basket_uuid {basket_uuid} " +
                "is listed as a parent UUID for another basket. Please " +
                "delete that basket before deleting it's parent basket."
            )

        remove_item = self.index_df[self.index_df["uuid"] == basket_uuid]
        self.file_system.rm(remove_item['address'].values[0], recursive=True)
        self.index_df.drop(remove_item.index, inplace=True)
        self.index_df.reset_index(drop=True, inplace=True)
        if upload_index:
            self._upload_index(self.index_df)

    def get_parents(self, basket, **kwargs):
        """Recursively gathers all parents of basket and returns index

        Parameters
        ----------
        basket: string
            string that holds the path of the basket
            can also be the basket uuid

        kwargs:
        gen_level: int
            number the indicates what generation we are on, 1 for parent,
            2 for grandparent and so forth
        data: pandas dataframe (optional)
            this is the index or dataframe we have collected so far
            when it is initially called, it is empty, every
            iteration/recursive call we add all the immediate parents for
            the given basket
        descendants: list of str
            this is a list that holds the uids of all the descendents the
            function has visited. this is used to prevent/detect any
            parent-child loops found in the basket structure.

        Returns
        ----------
        index or dataframe of all the parents of the immediate
        basket we are given, along with all the previous parents
        of the previous calls.
        """
        # collect info from kwargs
        gen_level = kwargs.get("gen_level", 1)
        data = kwargs.get("data", pd.DataFrame())
        descendants = kwargs.get("descendants", [])

        if self.sync:
            if not self.is_index_current():
                self.sync_index()
        elif self.index_df is None:
            self.sync_index()

        # validate the bucket exists. if it does,
        # make sure we use the address or the uid
        if (not self.file_system.exists(basket) and
            basket not in self.index_df.uuid.values):
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket}'"
            )

        if self.file_system.exists(basket):
            current_uid = self.index_df["uuid"].loc[
                self.index_df["address"].str.endswith(basket)
            ].values[0]
        elif basket in self.index_df.uuid.values:
            current_uid = basket

        # get all the parent uuids for the current uid
        puids = self.index_df["parent_uuids"].loc[
            self.index_df["uuid"] == current_uid
        ].to_numpy()[0]

        # check if the list is empty return the data how it is
        if len(puids) == 0:
            return data

        if current_uid in descendants:
            raise ValueError(f"Parent-Child loop found at uuid: {current_uid}")
        descendants.append(current_uid)

        parents_index = self.index_df.loc[
            self.index_df["uuid"].isin(puids), :
        ].copy()

        if len(parents_index) == 0:
            return data

        parents_index.loc[:, "generation_level"] = gen_level

        #add the parents for this generation to the data
        data = pd.concat([data, parents_index])

        # for every parent, go get their parents
        for basket_addr in parents_index["address"]:
            data = self.get_parents(basket=basket_addr,
                                    gen_level=gen_level+1,
                                    data=data,
                                    descendants=descendants.copy())
        return data

    def get_children(self, basket, **kwargs):
        """Recursively gathers all the children of basket and returns an index

        Parameters
        ----------
        basket: string
            string that holds the path of the basket
            can also be the basket uuid

        kwargs:
        gen_level: int
            number the indicates what generation we are on, -1 for child.
            -2 for grandchild and so forth
        data: pandas dataframe (optional)
            this is the index or dataframe we have collected so far
            when it is initially called, it is empty, every
            iteration/recursive call we add all the immediate children for
            the given basket
        ancestors: list of string
            this is a list of basket uuids of all the ancestors that have been
            visited. This is being used to detect if there is a parent-child
            loop inside the basket structure

        Returns
        ----------
        index or dataframe of all the children of the immediate
        basket we are given, along with all the previous children
        of the previous calls.
        """
        # collect info from kwargs
        gen_level = kwargs.get("gen_level", -1)
        data = kwargs.get("data", pd.DataFrame())
        ancestors = kwargs.get("ancestors", [])

        if self.sync:
            if not self.is_index_current():
                self.sync_index()
        elif self.index_df is None:
            self.sync_index()

        # validate the bucket exists. if it does,
        # make sure we use the address or the uid
        if (not self.file_system.exists(basket) and
            basket not in self.index_df.uuid.values):
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket}'"
            )

        if self.file_system.exists(basket):
            current_uid = self.index_df["uuid"].loc[
                self.index_df["address"].str.endswith(basket)
            ].values[0]
        elif basket in self.index_df.uuid.values:
            current_uid = basket

        # this looks at all the baskets and returns a list of baskets who have
        # the the parent id inside their "parent_uuids" list
        child_index = self.index_df.loc[
            self.index_df.parent_uuids.apply(lambda a: current_uid in a)
        ]

        cids = child_index["uuid"].values

        if len(cids) == 0:
            return data

        # we are storing all the ancestors in a list, if we find the same
        # ancestor twice, we are in a loop, throw error
        if current_uid in ancestors:
            raise ValueError(f"Parent-Child loop found at uuid: {current_uid}")
        ancestors.append(current_uid)

        # pandas is wanting me to make a copy of itself,
        # I'm not exactly sure why
        child_index = child_index.copy()
        child_index.loc[:, "generation_level"] = gen_level

        # add the children from this generation to the data
        data = pd.concat([data, child_index])

        # go through all the children and get their children too
        for basket_addr in child_index["address"]:
            data =  self.get_children(basket=basket_addr,
                                      gen_level=gen_level-1,
                                      data=data,
                                      ancestors=ancestors.copy())
        return data

    def upload_basket(self, upload_items, basket_type, **kwargs):
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
            and stored in the basket in upload file_system.
        label: optional str,
            Optional user friendly label associated with the basket.
        """
        parent_ids = kwargs.get("parent_ids", [])
        metadata = kwargs.get("metadata", {})
        label = kwargs.get("label", "")

        self.sync_index()
        up_dir = weave.uploader_functions.UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            file_system=self.file_system,
            bucket_name=self.bucket_name,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
        ).get_upload_path()
        single_indice_index = create_index_from_fs(up_dir, self.file_system)
        self._upload_index(
            pd.concat([self.index_df, single_indice_index], ignore_index=True)
        )
