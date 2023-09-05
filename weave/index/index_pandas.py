""" This module is for handling the pandas based backend of the Index object.
"""
import os
import tempfile
import warnings
from time import time_ns

import pandas as pd

from ..upload import UploadBasket
from .create_index import create_index_from_fs
from .index_abc import IndexABC

class PandasIndex(IndexABC):
    '''Handles Pandas based functionality of the Index'''

    def __init__(self, file_system, pantry_path, **kwargs):
        '''Initializes the Index class.

        Parameters
        ----------
        pantry_path: [string]
            Name of the bucket which the desired index is associated with.
        sync: [bool]
            Whether or not to check the index on disk to ensure this Index
            object stays current. If True, then some operations may take
            slightly longer, as they will check to see if the current Index
            object has the same information as the index on the disk. If False,
            then the Index object may be stale, but operations will perform
            at a higher speed.

        **file_system: fsspec object
            The fsspec object which hosts the bucket we desire to index.
            If file_system is None, then the default fs is retrieved from the
            config.
        '''
        super().__init__(file_system=file_system,
                         pantry_path=pantry_path,
                         **kwargs
        )
        self.index_basket_dir_name = 'index' # AKA basket type
        self.index_basket_dir_path = os.path.join(
            self.pantry_path, self.index_basket_dir_name
        )
        self.sync = bool(kwargs.get('sync', True))
        self.index_json_time = 0 # 0 is essentially same as None in this case
        self.index_df = None

    def __len__(self):
        """Returns the number of baskets in the index."""
        if self.sync and not self.is_index_current():
            self.sync_index()
        elif self.index_df is None:
            self.sync_index()
        return len(self.index_df)

    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'SQLIndex')."""
        return "PandasIndex"

    @property
    def file_system(self):
        """The file system of the pantry referenced by this Index."""
        return self._file_system

    @property
    def pantry_path(self):
        """The pantry path referenced by this Index."""
        return self._pantry_path

    def get_metadata(self, **kwargs):
        """Populates the metadata for the index.

        Parameters
        ----------
        Optional kwargs controlled by concrete implementations.
        """
        super().get_metadata(**kwargs)
        return self.metadata

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

    def to_pandas_df(self, max_rows=1000, **kwargs):
        """Returns the pandas dataframe representation of the index.

        Parameters
        ----------
        max_rows: int
            Max rows returned in the pandas dataframe.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame
            Returns a dataframe of the manifest data of the baskets in the
            pantry.
        """
        if self.sync:
            if not self.is_index_current():
                self.sync_index()
        elif self.index_df is None:
            self.sync_index()
        return self.index_df.head(max_rows)

    def clean_up_indices(self, n_keep=20):
        '''Deletes any index basket except the latest n index baskets.

        Parameters
        ----------
        n_keep: [int]
            n is the number of latest index baskets to keep.
        '''
        n_keep = int(n_keep)
        index_paths = self.file_system.glob(f"{self.index_basket_dir_path}"
                                            "/**/*-index.json")
        if len(index_paths) <= n_keep:
            return
        index_time_list = [self._get_index_time_from_path(i)
                           for i in index_paths]
        index_times_to_keep = sorted(index_time_list,
                                     reverse=True)[:n_keep]
        for index_time in index_time_list:
            if index_time not in index_times_to_keep:
                try:
                    path = self.file_system.glob(
                        f"{self.index_basket_dir_path}/**/" +
                        f"{index_time}-index.json"
                    )[0]
                    parent_path = path.rsplit(os.path.sep,1)[0]
                    self.file_system.rm(parent_path,recursive = True)
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

    def generate_index(self, **kwargs):
        """Populates the index from the file system.

        Generate the index by scraping the pantry and adding the manifest data
        of found baskets to the index.

        Parameters
        ----------
        Optional kwargs controlled by concrete implementations.
        """
        index = create_index_from_fs(self.pantry_path, self.file_system)
        self._upload_index(index=index)

    def _upload_index(self, index):
        """Upload a new index"""
        with tempfile.TemporaryDirectory() as out:
            n_secs = time_ns()
            temp_json_path = os.path.join(out, f"{n_secs}-index.json")
            index.to_json(temp_json_path, date_format='iso', date_unit='ns')
            UploadBasket(
                upload_items=[{'path':temp_json_path, 'stub':False}],
                basket_type=self.index_basket_dir_name,
                file_system=self.file_system,
                pantry_name=self.pantry_path
            )
        self.index_df = index
        self.index_json_time = n_secs


    def untrack_basket(self, basket_address, **kwargs):
        """Remove a basket from being tracked of given UUID or path.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Optional kwargs controlled by concrete implementations.
        """
        upload_index = kwargs.get("upload_index", True)
        basket_address = str(basket_address)
        if self.index_df is None:
            self.sync_index()

        remove_item = self.index_df[(self.index_df["uuid"] == basket_address)
                               | (self.index_df["address"] == basket_address)
                      ]

        self.index_df.drop(remove_item.index, inplace=True)
        self.index_df.reset_index(drop=True, inplace=True)
        if upload_index:
            self._upload_index(self.index_df)


    def get_parents(self, basket_address, **kwargs):
        """Recursively gathers all parents of basket and returns index

        Parameters
        ----------
        basket_address: string
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
        if (not self.file_system.exists(basket_address) and
            basket_address not in self.index_df.uuid.values):
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )

        if self.file_system.exists(basket_address):
            current_uid = self.index_df["uuid"].loc[
                self.index_df["address"].str.endswith(basket_address)
            ].values[0]
        elif basket_address in self.index_df.uuid.values:
            current_uid = basket_address

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
            data = self.get_parents(basket_address=basket_addr,
                                    gen_level=gen_level+1,
                                    data=data,
                                    descendants=descendants.copy())
        return data

    def get_children(self, basket_address, **kwargs):
        """Recursively gathers all the children of basket and returns an index

        Parameters
        ----------
        basket_address: string
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
        if (not self.file_system.exists(basket_address) and
            basket_address not in self.index_df.uuid.values):
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )

        if self.file_system.exists(basket_address):
            current_uid = self.index_df["uuid"].loc[
                self.index_df["address"].str.endswith(basket_address)
            ].values[0]
        elif basket_address in self.index_df.uuid.values:
            current_uid = basket_address

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
            data =  self.get_children(basket_address=basket_addr,
                                      gen_level=gen_level-1,
                                      data=data,
                                      ancestors=ancestors.copy())
        return data

    def track_basket(self, entry_df, **kwargs):
        """Track a basket to from the pantry referenced by the Index

        Parameters
        ----------
        entry_df : pd.DataFrame
        """
        self._upload_index(
            pd.concat([self.index_df, entry_df], ignore_index=True)
        )


    def get_row(self, basket_address, **kwargs):
        """Returns a pd.DataFrame row information of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        rows: pd.DataFrame
            Manifest information for the requested basket.
        """
        if self.index_df is None:
            self.generate_index()
        if not isinstance(basket_address, list):
            basket_address = [basket_address]
        rows = self.index_df[(self.index_df["uuid"].isin(basket_address))
                           | (self.index_df["address"].isin(basket_address))
                      ]
        return rows

    def get_baskets_of_type(self, basket_type, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets of basket_type.

        Parameters
        ----------
        basket_type: str
            The basket type to filter for.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets of the type.
        """
        return self.index_df[
            self.index_df["basket_type"] == basket_type
        ].head(max_rows)

    def get_baskets_of_label(self, basket_label, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the label
        """
        return self.index_df[
            self.index_df["label"] == basket_label
        ].head(max_rows)

    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   max_rows=1000, **kwargs):
        """Returns a pandas dataframe of baskets uploaded between two times.

        Parameters
        ----------
        start_time: datetime.datetime
            The start datetime object to filter between. If None, will filter
            from the beginning of time.
        end_time: datetime.datetime
            The end datetime object to filter between. If None, will filter
            to the current datetime.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets uploaded
        between the start and end times.
        """
        if start_time is None and end_time is None:
            raise ValueError("Either start time or end time must not be None")

        if start_time is None:
            return self.index_df[
                self.index_df["upload_time"] <= end_time
            ].head(max_rows)

        if end_time is None:
            return self.index_df[
                self.index_df["upload_time"] >= start_time
            ].head(max_rows)

        return self.index_df[ (self.index_df["upload_time"] >= start_time)
                             & (self.index_df["upload_time"] <= end_time)
            ].head(max_rows)

    def query(self, expr, **kwargs):
        """Returns a pandas dataframe of the results of the query.

        Parameters
        ----------
        expr: str
           Pass SQL Query to the pandas dataframe

        Returns
        ----------
        pandas.DataFrame of the resulting query.
        """
        return self.index_df.query(expr)
