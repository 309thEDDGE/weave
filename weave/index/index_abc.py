"""
Wherein is contained the Abstract Base Class for Index.
"""
import abc

from ..config import get_file_system


class IndexABC(abc.ABC):
    """Abstract Base Class for the Index"""
    def __init__(self, **kwargs):
        self.file_system = kwargs.get("file_system", get_file_system())
        self.pantry_path = str(kwargs['pantry_path'])

#     @property
#     @abc.abstractmethod
#     def file_system(self):
#         """The file system of the pantry referenced by this Index."""

#     @property
#     @abc.abstractmethod
#     def pantry_path(self):
#         """The pantry path referenced by this Index."""

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

    @abc.abstractmethod
    def upload_basket(self, upload_items, basket_type, **kwargs):
        """Upload a basket to the pantry referenced by the Index.

        Parameters
        ----------
        upload_items : [dict]
            List of python dictionaries with the following schema:
            {
                'path': path to the file or folder being uploaded (str),
                'stub': True/False (bool)
            }
            'path' can be a file or folder to be uploaded. Every filename
            and folder name must be unique. If 'stub' is set to True, integrity
            data will be included without uploading the actual file or folder.
            Stubs are useful when original file source information is desired
            without uploading the data itself. This is especially useful when
            dealing with large files.
        basket_type: str
            Type of basket being uploaded.

        Optional kwargs controlled by concrete implementations.
        """

    @abc.abstractmethod
    def delete_basket(self, basket_address, **kwargs):
        """Deletes a basket of given UUID or path.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Optional kwargs controlled by concrete implementations.
        """

    @abc.abstractmethod
    def get_basket(self, basket_address, **kwargs):
        """Returns a Basket of given UUID or path.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        Basket
            Returns the Basket object.
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

    @abc.abstractmethod
    def get_baskets_of_label(self, basket_label, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Optional kwargs controlled by concrete implementations.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the label
        """

    @abc.abstractmethod
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
        """Returns the str instantiation type of this Index (ie 'SQLIndex')."""
