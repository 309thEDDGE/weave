"""
USAGE:
python create_index.py <root_dir>
    root_dir: the root directory of s3 you wish to build your index off of
"""
from ..basket import Basket
from .index_pandas import _Index


class Index(_Index):
    '''Facilitate user interaction with the index of a Weave data warehouse.'''
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
        return Basket(basket_address, self.bucket_name,
                      file_system=self.file_system)
