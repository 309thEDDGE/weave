import abc

class IndexABC(abc.ABC):
    @property
    @abstractmethod
    def file_system(self):
        """The file system of the pantry referenced by this Index."""
        pass

    @abc.abstractmethod
    def generate_index(self):
        """Populates the index from the file system."""
        pass

    @abc.abstractmethod
    def to_pandas_df(self, **kwargs):
        """Returns the pandas dataframe representation of the index."""
        pass

    @abc.abstractmethod
    def upload_basket(self, upload_items, basket_type, **kwargs):
        """Upload a basket to the same pantry referenced by the Index."""
        pass

    @abc.abstractmethod
    def delete_basket(self, basket_address, **kwargs):
        """Deletes a basket of given UUID or path."""
        pass

    @abc.abstractmethod
    def get_basket(self, basket_address):
        """Returns a Basket of given UUID or path."""
        pass

    @abc.abstractmethod
    def get_parents(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all parents of a basket."""
        pass

    @abc.abstractmethod
    def get_children(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all children of a basket."""
        pass

    @abc.abstractmethod
    def get_baskets_of_type(self, basket_type):
        """Returns a pandas dataframe containing baskets of basket_type."""
        pass

    @abc.abstractmethod
    def get_baskets_of_label(self, basket_label):
        """Returns a pandas dataframe containing baskets with label."""
        pass

    @abc.abstractmethod
    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   **kwargs):
        """Returns a pandas dataframe containing baskets with label."""
        pass

    @abc.abstractmethod
    def query(self, query, **kwargs):
        """Returns a pandas dataframe of the results of the query."""
        pass

    @abc.abstractmethod
    def __len__(self):
        """Returns the number of baskets in the index."""
        pass
