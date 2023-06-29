import os
import json
from pathlib import Path

from weave import config, Index


class Basket:
    """This class provides convenience functions for accessing basket contents.
    """

    def __init__(self, basket_address, bucket_name="basket-data"):
        """Initializes the Basket_Class.

        Parameters
        ----------
        basket_address: string
            Argument can take one of two forms: either a path to the Basket 
            directory, or the UUID of the basket.
        """
        self.fs = config.get_file_system()
        try:
            self.set_up_basket_from_path(basket_address)
        except ValueError as e:
            if e.__str__() != f"Basket does not exist: {self.basket_address}":
                raise e
            else:
                self.set_up_basket_from_uuid(basket_address, bucket_name)
        self.manifest_path = f"{self.basket_address}/basket_manifest.json"
        self.supplement_path = f"{self.basket_address}/basket_supplement.json"
        self.metadata_path = f"{self.basket_address}/basket_metadata.json"
        self.manifest = None
        self.supplement = None
        self.metadata = None
        self.validate()

    def set_up_basket_from_path(self, basket_address):
        self.basket_address = os.fspath(basket_address)
        self.validate_basket_path()

    def set_up_basket_from_uuid(self, basket_address, bucket_name):
        ind = Index(bucket_name=bucket_name)
        ind_df = ind.to_pandas_df()
        ind_df[""]

    def validate_basket_path(self):
        """Validates basket health"""
        if not self.fs.exists(self.basket_address):
            raise ValueError(f"Basket does not exist: {self.basket_address}")

    def validate(self):
        """Validates basket health"""
        if not self.fs.exists(self.manifest_path):
            raise FileNotFoundError(
                f"Invalid Basket, basket_manifest.json "
                f"does not exist: {self.manifest_path}"
            )

        if not self.fs.exists(self.supplement_path):
            raise FileNotFoundError(
                f"Invalid Basket, basket_supplement.json "
                f"does not exist: {self.supplement_path}"
            )

    def get_manifest(self):
        """Return basket_manifest.json as a python dictionary"""
        if self.manifest is not None:
            return self.manifest

        with self.fs.open(self.manifest_path, "rb") as file:
            self.manifest = json.load(file)
            return self.manifest

    def get_supplement(self):
        """Return basket_supplement.json as a python dictionary"""
        if self.supplement is not None:
            return self.supplement

        with self.fs.open(self.supplement_path, "rb") as file:
            self.supplement = json.load(file)
            return self.supplement

    def get_metadata(self):
        """Return basket_metadata.json as a python dictionary

        Return None if metadata doesn't exist
        """
        if self.metadata is not None:
            return self.metadata

        if self.fs.exists(self.metadata_path):
            with self.fs.open(self.metadata_path, "rb") as file:
                self.metadata = json.load(file)
                return self.metadata
        else:
            return None

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
        relative_path: [string]
            relative path in the basket to pass to filesystem.ls.

        Returns
        ---------
        filesystem.ls results of the basket.
        """

        ls_path = os.fspath(Path(self.basket_address))

        if relative_path is not None:
            relative_path = os.fspath(relative_path)
            ls_path = os.fspath(
              Path(os.path.join(self.basket_address, relative_path))
            )

        if ls_path == os.fspath(Path(self.basket_address)): 
            # remove any prohibited files from the list if they exist 
            # in the root directory
            # Note that fs.ls can have unpredictable behavior if
            # not passing refresh=True
            ls_results = self.fs.ls(ls_path, refresh=True)
            return [
                x
                for x in ls_results
                if os.path.basename(Path(x)) not in config.prohibited_filenames
            ]
        else:
            # Note that fs.ls can have unpredictable behavior if
            # not passing refresh=True
            return self.fs.ls(ls_path, refresh=True)
