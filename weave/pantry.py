"""Wherein is contained the Pantry class.
"""
import json
import os
import warnings

import s3fs
# Ignore pylint duplicate code. Code here is used to explicitly show pymongo is
# an optional dependency. Duplicate code is found in config.py (where pymongo
# is actually imported)
# pylint: disable-next=duplicate-code
try:
    # For the sake of explicitly showing that pymongo is optional, import
    # pymongo here, even though it is not currently used in this file.
    # Pylint ignore the next unused-import pylint warning.
    # Also inline ruff ignore unused import (F401)
    # pylint: disable-next=unused-import
    import pymongo  # noqa: F401
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

from .mongo_loader import MongoLoader
from .basket import Basket
from .config import get_file_system
from .index.create_index import create_index_from_fs
from .index.index_abc import IndexABC
from .upload import UploadBasket, derive_integrity_data
from .validate import validate_pantry

# pylint: disable-next=too-many-instance-attributes
class Pantry():
    """Facilitate user interaction with the index of a Weave data warehouse.
    """

    def __init__(self, index: IndexABC, pantry_path="weave-test",
                 mongo_client=None, **kwargs):
        """Initialize Pantry object.

        A pantry is a collection of baskets. This class facilitates the upload,
        retrieval, and deletion of baskets within a file system. It uses and
        updates an index to track the contents within the pantry for rapid
        operations.

        Parameters:
        -----------
        index: IndexABC
            The concrete implementation of an IndexABC. This is used to track
            the contents within the pantry.
        pantry_path: str (default="weave-test")
            Name of the pantry this object is associated with.
        mongo_client: pymongo.MongoClient (optional)
            The MongoClient object to be used to update a mongo datastore when
            adding and removing baskets.
        **file_system: fsspec object (optional)
            The fsspec object which hosts the pantry we desire to index.
            If file_system is None, then the default fs is retrieved from the
            config.
        **mongo_config: dict (optional)
            A dictionary containing keys and values to be used for the mongo
            collection. Supported keys: MONGODB_HOST, MONGODB_PORT,
            MONGODB_USERNAME, MONGODB_PASSWORD, METADATA_COLLECTION,
            MANIFEST_COLLECTION, SUPPLEMENT_COLLECTION.
            If keys are not present, default values will be used if necessary.
        """
        self.file_system = kwargs.pop("file_system", None)
        if self.file_system is None:
            self.file_system = get_file_system()

        # If using S3FileSystem, check the connection
        if isinstance(self.file_system, s3fs.S3FileSystem):
            try:
                self.file_system.ls('s3://')
            except Exception as exc:
                raise ConnectionError("Connection to s3fs failed.") from exc

        try:
            if not self.file_system.exists(pantry_path):
                self.file_system.mkdir(pantry_path)
        except Exception as exc:
            raise OSError("Failed to create directory, Invalid"
                            f" Path {pantry_path}.\n{exc}") from exc

        self.pantry_path = str(pantry_path)
        self.setup_config = {}
        self.load_setup_config()

        # Check if file system is read-only. If so, raise error.
        try:
            self.file_system.touch(os.path.join(self.pantry_path,
                                                "test_read_only.txt"))
            self.file_system.rm(os.path.join(self.pantry_path,
                                                "test_read_only.txt"))
            self.is_read_only = False
        except (OSError, ValueError):
            self.is_read_only = True

        self.index = index(
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            setup_config=self.setup_config['index_setup_config'],
            pantry_read_only=self.is_read_only,
            **kwargs,
        )
        self.mongo_client = mongo_client
        self.mongo_config = kwargs.get("mongo_config", {})
        self.setup_config['index_setup_config'] = self.index.generate_config()
        self._generate_config()

    def validate_path_in_pantry(self, path):
        """Validate the given path is within the pantry.

        Check 1: Ensure the path begins with the pantry path.
        Check 2: Ensure the ".." navigation command is not used.

        Parameters:
        -----------
        path: str
            Path to verify.
        """
        valid = True
        if self.pantry_path:
            valid = path.startswith(self.pantry_path + os.path.sep)
        if valid and "zip" not in str(type(self.file_system)):
            bad_str = os.path.sep + ".." + os.path.sep
            valid = bad_str not in path

        if not valid:
            raise ValueError(
                f"Attempting to access basket outside of pantry: {path}"
            )

    def load_metadata(self):
        """(Deprecated) Load pantry metadata from config.json."""
        warnings.warn(
            UserWarning("This function is being deprecated in a future weave "
                        "release. Please use load_setup_config() instead.")
        )
        self.load_setup_config()

    def load_setup_config(self):
        """Load pantry setup configuration from config.json."""
        self.config_path = os.path.join(
            self.pantry_path,
            'config.json',
        )
        if self.file_system.exists(self.config_path):
            with self.file_system.open(self.config_path, "rb") as file:
                self.setup_config = json.load(file)
        else:
            self.setup_config = {}
        if 'index_setup_config' not in self.setup_config:
            self.setup_config['index_setup_config'] = {}

    def _generate_config(self):
        """Helper method to populate the setup_config dictionary."""
        self.setup_config["index"] = str(self.index)
        self.setup_config["file_system"] = str(
            self.file_system.__class__.__name__
        )
        self.setup_config["pantry_path"] = self.pantry_path

        # Add the provided mongo_config to the main config dictionary.
        if self.mongo_config:
            self.setup_config.update(self.mongo_config)

    def save_metadata(self):
        """(Deprecated) Dump metadata to to pantry metadata file."""
        warnings.warn(
            UserWarning("This function is being deprecated in a future weave "
                        "release. Please use save_setup_config() instead.")
        )
        self.save_setup_config()

    def save_setup_config(self):
        """Save setup configuration settings to a config file inside the
        pantry."""
        with self.file_system.open(
            self.config_path, "w", encoding="utf-8"
        ) as outfile:
            json.dump(self.setup_config, outfile)

    def validate(self):
        """Convenient wrapper function to validate the pantry.

        Returns
        ----------
        A list of all invalid basket locations (will return an empty list if
        no warnings are raised)
        """

        return validate_pantry(self)

    def delete_basket(self, basket_address, **kwargs):
        """Deletes basket of given UUID or path.

        Note that the given basket will not be deleted if the basket is listed
        as the parent uuid for any of the baskets in the index.

        Parameters:
        -----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        **kwargs:
            Additional parameters to pass to the index
        """

        basket_address = str(basket_address)
        remove_item = self.index.get_rows(basket_address)

        if len(self.index.get_children(remove_item.iloc[0].uuid)) > 0:
            raise ValueError(
                f"The provided value for basket_uuid {basket_address} " +
                "is listed as a parent UUID for another basket. Please " +
                "delete that basket before deleting its parent basket."
            )

        self.validate_path_in_pantry(remove_item.iloc[0].address)
        self.index.untrack_basket(remove_item.iloc[0].address, **kwargs)
        self.file_system.rm(remove_item.iloc[0].address, recursive=True)

        if self.mongo_client is not None:
            MongoLoader(self).remove_document(remove_item.iloc[0].uuid)

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
        # Check if file system is read-only. If so, raise error.
        if self.is_read_only:
            raise ValueError(
                "Unable to upload a basket to a read-only file system."
            )

        parent_ids = kwargs.pop("parent_ids", [])
        metadata = kwargs.pop("metadata", {})
        label = kwargs.pop("label", "")

        up_dir = UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
            **kwargs,
        ).get_upload_path()

        single_indice_index = create_index_from_fs(up_dir, self.file_system)
        self.index.track_basket(single_indice_index)

        if self.mongo_client is not None:
            MongoLoader(self).\
                load_mongo(single_indice_index.iloc[0].uuid)

        return single_indice_index

    def get_basket(self, basket_address):
        """Retrieves a basket of given UUID or path.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the Basket
            directory, or the UUID of the basket.

        Returns
        ----------
        The Basket object associated with the given UUID or path.
        """

        # Create a Basket from the given address, and the index's file_system
        # and pantry name. Basket will catch invalid inputs and raise
        # appropriate errors.
        row = self.index.get_rows(basket_address)
        if len(row) == 0:
            raise ValueError(f"Basket does not exist: {basket_address}")
        self.validate_path_in_pantry(row.iloc[0].address)
        return Basket(row.iloc[0].address, pantry=self)

    def does_file_exist(self, file_path, **kwargs):
        """Check if a file already exists inside the pantry and return
        the uuids where it does.

        Parameters
        ----------
        file_path: str
            Local path to file being checked
        **FORCE_LOAD_SUPPLEMENT: bool (default=false)
            If true, the mongo supplement will be reloaded with the ENTIRETY of
            the current pantry contents.

        Returns
        ----------
        list of basket uuids of where the file exists if it does. If file does
        not exist in the pantry, an empty list is returned.
        """
        if not _HAS_PYMONGO:
            raise ImportError("Missing Dependency. The package 'pymongo' "
                              "is required to use this function.")

        integrity_data = derive_integrity_data(file_path)['hash']
        try:
            mongo_loader = MongoLoader(pantry=self)
        except pymongo.errors.ServerSelectionTimeoutError:
            return []

        if kwargs.get("FORCE_LOAD_SUPPLEMENT", False):
            mongo_loader.load_mongo_supplement(
                self.index.to_pandas_df().uuid.to_list()
            )

        supplement_collection = mongo_loader.database['supplement']

        uuids = [f['uuid'] for f in supplement_collection.find(
            {'integrity_data.hash': {'$in': [integrity_data]}},
            {'uuid':1, '_id':0}
        )]

        return uuids
