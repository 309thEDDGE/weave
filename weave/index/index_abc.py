"""
Wherein is contained the Abstract Base Class for Index.
"""
import abc


class IndexABC(abc.ABC):
    """Abstract Base Class for the Index"""
    @property
    @abc.abstractmethod
    def file_system(self):
        """The file system of the pantry referenced by this Index."""

    @property
    @abc.abstractmethod
    def pantry_name(self):
        """The pantry name referenced by this Index."""

    @abc.abstractmethod
    def generate_index(self):
        """Populates the index from the file system."""

    @abc.abstractmethod
    def to_pandas_df(self, **kwargs):
        """Returns the pandas dataframe representation of the index."""

    @abc.abstractmethod
    def upload_basket(self, upload_items, basket_type, **kwargs):
        """Upload a basket to the same pantry referenced by the Index."""

    @abc.abstractmethod
    def delete_basket(self, basket_address, **kwargs):
        """Deletes a basket of given UUID or path."""

    @abc.abstractmethod
    def get_basket(self, basket_address):
        """Returns a Basket of given UUID or path."""

    @abc.abstractmethod
    def get_parents(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all parents of a basket."""

    @abc.abstractmethod
    def get_children(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all children of a basket."""

    @abc.abstractmethod
    def get_baskets_of_type(self, basket_type):
        """Returns a pandas dataframe containing baskets of basket_type."""

    @abc.abstractmethod
    def get_baskets_of_label(self, basket_label):
        """Returns a pandas dataframe containing baskets with label."""

    @abc.abstractmethod
    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   **kwargs):
        """Returns a pandas dataframe of baskets uploaded between two times."""

    @abc.abstractmethod
    def query(self, query, **kwargs):
        """Returns a pandas dataframe of the results of the query."""

    @abc.abstractmethod
    def __len__(self):
        """Returns the number of baskets in the index."""
