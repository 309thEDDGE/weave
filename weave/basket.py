"""Wherein scripts concerning the Basket class reside
"""

import json
import os
from pathlib import Path

from .config import get_file_system, prohibited_filenames
from .index.index_pandas import _Index


class BasketInitializer:
    """Initializes basket class. Validates input args.
    """
    def __init__(self, basket_address, pantry_name, **kwargs):
        """Handles set up of basket. Calls validation.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        pantry_name: str
            Name of the pantry which the desired index is associated with.

        **file_system: fsspec object
            The fsspec filesystem to be used for retrieving and uploading.
        """
        self.file_system = kwargs.get("file_system", get_file_system())
        try:
            self._set_up_basket_from_path(basket_address)
        except ValueError as error:
            if str(error) != f"Basket does not exist: {self.basket_path}":
                raise error
            self._set_up_basket_from_uuid(basket_address, pantry_name)
        self.manifest_path = f"{self.basket_path}/basket_manifest.json"
        self.supplement_path = f"{self.basket_path}/basket_supplement.json"
        self.metadata_path = f"{self.basket_path}/basket_metadata.json"
        self.validate()

    def _set_up_basket_from_path(self, basket_address):
        """Attempts to set up a basket from a filepath.

        Paramters
        ---------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. In this case it is assumed to
            be a path to the basket directory.
        """
        self.basket_path = os.fspath(basket_address)
        self.validate_basket_path()

    def _set_up_basket_from_uuid(self, basket_address, pantry_name):
        """Attempts to set up a basket from a uuid.

        Note that if the basket cannot be set up from a uuid then an attempt to
        set up the basket from a filepath will be made.

        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. In this case it is assumed to
            be the UUID of the basket.
        pantry_name: str
            Name of the pantry which the desired index is associated with.
        """
        try:
            ind = _Index(pantry_name=pantry_name, file_system=self.file_system)
            ind_df = ind.to_pandas_df()
            path = ind_df["address"][ind_df["uuid"] == basket_address].iloc[0]
            self._set_up_basket_from_path(basket_address=path)
        except BaseException as error:
            self.basket_path = basket_address
            self.validate_basket_path()
            # The above line should raise an exception
            # The below line is more or less a fail safe and will raise the ex.
            raise error

    def validate_basket_path(self):
        """Validates basket exists"""
        if not self.file_system.exists(self.basket_path):
            raise ValueError(f"Basket does not exist: {self.basket_path}")

    def validate(self):
        """Validates basket health"""
        if not self.file_system.exists(self.manifest_path):
            raise FileNotFoundError(
                f"Invalid Basket, basket_manifest.json "
                f"does not exist: {self.manifest_path}"
            )

        if not self.file_system.exists(self.supplement_path):
            raise FileNotFoundError(
                f"Invalid Basket, basket_supplement.json "
                f"does not exist: {self.supplement_path}"
            )

# Ignoring because there is a necessary and
# reasonable amount of variables in this case
# pylint: disable=too-many-instance-attributes
class Basket(BasketInitializer):
    """This class provides convenience functions for accessing basket contents.
    """

    def __init__(self, basket_address, pantry_name="basket-data", **kwargs):
        """Initializes the Basket_Class.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        pantry_name: str
            Name of the pantry which the desired index is associated with.

        **file_system: fsspec object
            The fsspec filesystem to be used for retrieving and uploading.
        """
        super().__init__(basket_address, pantry_name, **kwargs)

        self.manifest = None
        self.supplement = None
        self.metadata = None
        self.get_manifest()
        self.populate_members()

    def populate_members(self):
        """Populate the Basket_Class member variables"""
        self.uuid = self.manifest["uuid"]
        self.upload_time = self.manifest["upload_time"]
        self.parent_uuids = self.manifest["parent_uuids"]
        self.basket_type = self.manifest["basket_type"]
        self.label = self.manifest["label"]
        self.address = self.basket_path
        self.storage_type = self.file_system.__class__.__name__

    def get_manifest(self):
        """Return basket_manifest.json as a python dictionary"""
        if self.manifest is not None:
            return self.manifest

        with self.file_system.open(self.manifest_path, "rb") as file:
            self.manifest = json.load(file)
            return self.manifest

    def get_supplement(self):
        """Return basket_supplement.json as a python dictionary"""
        if self.supplement is not None:
            return self.supplement

        with self.file_system.open(self.supplement_path, "rb") as file:
            self.supplement = json.load(file)
            return self.supplement

    def get_metadata(self):
        """Return basket_metadata.json as a python dictionary

        Return None if metadata doesn't exist
        """
        if self.metadata is not None:
            return self.metadata

        if self.file_system.exists(self.metadata_path):
            with self.file_system.open(self.metadata_path, "rb") as file:
                self.metadata = json.load(file)
                return self.metadata
        else:
            return None

    # Disabling pylint name warning for ls, as it is the standard name
    # for functions of it's type in the computing world. It makes
    # sense to continue to name this function ls.
    # pylint: disable-next=invalid-name
    def ls(self, relative_path=None):
        """List directories and files in the basket.

           Call filesystem.ls relative to the basket directory.
           When relative_path = None, filesystem.ls is invoked
           from the base directory of the basket. If there are folders
           within the basket, relative path can be used to observe contents
           within folders. Example: if there exists a folder with the
           name 'folder1' within the basket, 'folder1' can be passed
           as the relative path to get back the filesystem.ls results
           of 'folder1'.

        Parameters
        ----------
        relative_path: str (default = None)
            relative path in the basket to pass to filesystem.ls.

        Returns
        ---------
        filesystem.ls results of the basket.
        """

        ls_path = os.fspath(Path(self.basket_path))

        if relative_path is not None:
            relative_path = os.fspath(relative_path)
            ls_path = os.fspath(
              Path(os.path.join(self.basket_path, relative_path))
            )

        if ls_path == os.fspath(Path(self.basket_path)):
            # Remove any prohibited files from the list if they exist
            # in the root directory
            # Note that file_system.ls can have unpredictable behavior if
            # not passing refresh=True
            ls_results = self.file_system.ls(ls_path, refresh=True)
            return [
                x
                for x in ls_results
                if os.path.basename(Path(x)) not in prohibited_filenames
            ]
        # Note that file_system.ls can have unpredictable behavior if
        # not passing refresh=True
        return self.file_system.ls(ls_path, refresh=True)
