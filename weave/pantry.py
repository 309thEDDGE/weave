"""
This class builds the user-facing Index class. It pulls from the _Index class
which uses Pandas as it's backend to build and interface with the on disk
Index baskets.
"""

from .basket import Basket
from .config import get_file_system
from .index.create_index import create_index_from_fs
from .index.index_abc import IndexABC
from .upload import UploadBasket
from .validate import validate_pantry

class Pantry():
    """Facilitate user interaction with the index of a Weave data warehouse.
    """

    def __init__(self, index: IndexABC, pantry_path="basket-data", **kwargs):
        """Initialize Pantry object

        A pantry is a collection of baskets. This class facilitates the upload,
        retrieval, and deletion of baskets within a file system. It uses and
        updates an index to track the contents within the pantry for rapid
        operations.

        Parameters:
        -----------
        index: IndexABC
            The concrete implementation of an IndexABC. This is used to track
            the contents within the pantry.
        pantry_path: str
            Name of the pantry this object is associated with.
        **file_system: fsspec object
            The fsspec object which hosts the bucket we desire to index.
            If file_system is None, then the default fs is retrieved from the
            config.
        """
        self.file_system = kwargs.get("file_system", get_file_system())
        self.pantry_path = str(pantry_path)
        self.index = index(file_system=self.file_system,
                           pantry_path=self.pantry_path,
        )

    def validate(self):
        """Convenient wrapper function to validate the pantry.

        Returns
        ----------
        A list of all invalid basket locations (will return an empty list if
        no warnings are raised)
        """
        return validate_pantry(self)

    def delete_basket(self, basket_address, **kwargs):
        '''Deletes basket of given UUID or path.

        Note that the given basket will not be deleted if the basket is listed
        as the parent uuid for any of the baskets in the index.

        Parameters:
        -----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        **kwargs:
            Additional parameters to pass to the index
        '''
        basket_address = str(basket_address)
        remove_item = self.index.get_row(basket_address)

        if len(self.index.get_children(remove_item.iloc[0].uuid)) > 0:
            raise ValueError(
                f"The provided value for basket_uuid {basket_address} " +
                "is listed as a parent UUID for another basket. Please " +
                "delete that basket before deleting it's parent basket."
            )
        self.file_system.rm(remove_item.iloc[0].address, recursive=True)
        self.index.untrack_basket(remove_item.iloc[0].address, **kwargs)

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
        **parent_ids: [str] (optional)
            List of unique ids associated with the parent baskets
            used to derive the new basket being uploaded.
        **metadata: dict (optional)
            Python dictionary that will be written to metadata.json
            and stored in the basket in upload file_system.
        **label: str (optional)
            Optional user friendly label associated with the basket.
        """
        parent_ids = kwargs.get("parent_ids", [])
        metadata = kwargs.get("metadata", {})
        label = kwargs.get("label", "")

        self.index.sync_index()
        up_dir = UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            file_system=self.file_system,
            pantry_name=self.pantry_path,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
        ).get_upload_path()

        single_indice_index = create_index_from_fs(up_dir, self.file_system)
        self.index.track_basket(single_indice_index)
        return single_indice_index

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
        row = self.index.get_row(basket_address)
        if len(row) == 0:
            raise ValueError(f"Basket does not exist: {basket_address}")
        return Basket(row.iloc[0].address, pantry=self)
