"""Wherein is contained the Abstract Base Class for Index.
"""

import abc
from datetime import datetime


class IndexABC(abc.ABC):
    """Abstract Base Class for the Index."""

    @abc.abstractmethod
    def __init__(self, file_system, pantry_path, **kwargs):
        """Initializes the Index class.

        Parameters
        ----------
        file_system: fsspec object
            The fsspec object which hosts the pantry we desire to index.
        pantry_path: str
            Path to the pantry root which we want to index.
        **metadata: dict (required)
            Existing metadata for the Index.
        Optional kwargs controlled by concrete implementations.
        """

        self._file_system = file_system
        self._pantry_path = pantry_path
        self.metadata = kwargs.get("metadata", {})
        self.generate_metadata()

    @property
    @abc.abstractmethod
    def file_system(self):
        """The file system of the pantry referenced by this Index."""

    @property
    @abc.abstractmethod
    def pantry_path(self):
        """The pantry path referenced by this Index."""

    @abc.abstractmethod
    def generate_metadata(self, **kwargs):
        """Generate the metadata for the index.

        Parameters
        ----------
        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        A dictionary of metadata for the index.
        """

        self.metadata["name"] = str(self)

    @abc.abstractmethod
    def generate_index(self, **kwargs):
        """Populates the index from the file system.

        Generate the index by scraping the pantry and adding the manifest data
        of found baskets to the index.

        Parameters
        ----------
        Optional kwargs controlled by concrete implementations.
        """

    @abc.abstractmethod
    def to_pandas_df(self, max_rows=1000, offset=0, **kwargs):
        """Returns the pandas dataframe representation of the index.

        Parameters
        ----------
        max_rows: int
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        Returns a pandas dataframe of the manifest data of the baskets in the
        pantry.
        """

    @abc.abstractmethod
    def track_basket(self, entry_df, **kwargs):
        """Track a basket (or many baskets) from the pantry with the Index.

        Parameters
        ----------
        entry_df : pandas.DataFrame
            Uploaded baskets to append to the index.
        Optional kwargs controlled by concrete implementations.
        """

    @abc.abstractmethod
    def untrack_basket(self, basket_address, **kwargs):
        """Remove a basket from being tracked of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.
        Optional kwargs controlled by concrete implementations.
        """

    @abc.abstractmethod
    def get_rows(self, basket_address, **kwargs):
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
        pandas.DataFrame of the Manifest information for the requested
        basket(s).
        """

    @abc.abstractmethod
    def get_parents(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all parents of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of parents (and recursively their parents) of the given basket.
        """

    @abc.abstractmethod
    def get_children(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all children of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of children (and recursively their children) of the given basket.
        """

    @abc.abstractmethod
    def get_baskets_of_type(self, basket_type, max_rows=1000,
                            offset=0, **kwargs):
        """Returns a pandas dataframe containing baskets of basket_type.

        Parameters
        ----------
        basket_type: str
            The basket type to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets of the type.
        """

    @abc.abstractmethod
    def get_baskets_of_label(self, basket_label, max_rows=1000,
                             offset=0, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the
        label.
        """

    @abc.abstractmethod
    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   max_rows=1000, offset=0, **kwargs):
        """Returns a pandas dataframe of baskets uploaded between two times.

        Parameters
        ----------
        start_time: datetime.datetime (default=None)
            The start datetime object to filter between. If None, will filter
            from the beginning of time.
        end_time: datetime.datetime (default=None)
            The end datetime object to filter between. If None, will filter
            to the current datetime.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets uploaded
        between the start and end times.
        """

        if start_time is not None and not isinstance(start_time,datetime):
            raise ValueError("start_time is not datetime object.")
        if end_time is not None and not isinstance(end_time,datetime):
            raise ValueError("end_time is not datetime object.")

    @abc.abstractmethod
    def query(self, expr, **kwargs):
        """Returns a pandas dataframe of the results of the query.

        Parameters
        ----------
        expr: str
            An expression passed to the backend. An example could be a SQL or
            pandas query. Largely dependent on concrete implementations.
        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame of the resulting query.
        """

    @abc.abstractmethod
    def __len__(self):
        """Returns the number of baskets in the index."""

    @abc.abstractmethod
    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'IndexSQL')."""
