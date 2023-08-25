"""
This class builds the user-facing Index class. It pulls from the _Index class
which uses Pandas as it's backend to build and interface with the on disk
Index baskets.
"""
# TODO: Reorder these
from ..upload import UploadBasket
from .index_abc import IndexABC
from ..config import get_file_system
from .create_index import create_index_from_fs

class Pantry():
    """Facilitate user interaction with the index of a Weave data warehouse."""
    def __init__(self, index: IndexABC, pantry_name="basket-data", **kwargs):
        self.index = index
        self.file_system = kwargs.get("file_system", get_file_system())
        self.pantry_name = str(pantry_name)

    def generate_index(self):
        '''Generates index and stores it in a basket'''
        return self.index.generate_index()

    def delete_basket(self, basket_uuid, **kwargs):
        '''Deletes basket of given UUID.

        Note that the given basket will not be deleted if the basket is listed
        as the parent uuid for any of the baskets in the index.

        Parameters:
        -----------
        basket_uuid: int
            The uuid of the basket to delete.
        **kwargs:
            Additional parameters to pass to the index
        '''
        basket_uuid = str(basket_uuid)

        # TODO: Should this error be raised in the basket class?
        # if basket_uuid not in self.index_df["uuid"].to_list():
        #     raise ValueError(
        #         f"The provided value for basket_uuid {basket_uuid} " +
        #         "does not exist."
        #     )
        if len(self.index.get_children(basket_uuid)) > 0:
            raise ValueError(
                f"The provided value for basket_uuid {basket_uuid} " +
                "is listed as a parent UUID for another basket. Please " +
                "delete that basket before deleting it's parent basket."
            )

        remove_item = self.index.get_basket(basket_uuid)
        self.file_system.rm(remove_item.address, recursive=True)
        self.index.delete_basket(basket_uuid, **kwargs)

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
        **parent_ids: optional [str]
            List of unique ids associated with the parent baskets
            used to derive the new basket being uploaded.
        **metadata: optional dict,
            Python dictionary that will be written to metadata.json
            and stored in the basket in upload file_system.
        **label: optional str,
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
            pantry_name=self.pantry_name,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
        ).get_upload_path()

        single_indice_index = create_index_from_fs(up_dir, self.file_system)
        self.index.upload_basket(single_indice_index)
        return single_indice_index

    # def get_basket(self, basket_address):
    #     """Retrieves a basket of given UUID or path.

    #     Parameters
    #     ----------
    #     basket_address: string
    #         Argument can take one of two forms: either a path to the Basket
    #         directory, or the UUID of the basket.

    #     Returns
    #     ----------
    #     The Basket object associated with the given UUID or path.
    #     """
    #     # Create a Basket from the given address, and the index's file_system
    #     # and bucket name. Basket will catch invalid inputs and raise
    #     # appropriate errors.
    #     return Basket(basket_address, self.pantry_name,
    #                   file_system=self.file_system)
