"""Contains scripts concerning the Basket class.
"""

import json
import os
import uuid
import importlib
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import s3fs

from .config import get_file_system, prohibited_filenames
from .validate import validate_basket_in_place_directory
from .validate import validate_basket_in_place_directory_backward
from .upload import derive_integrity_data




class BasketInitializer:
    """Initializes basket class. Validates input args."""

    def __init__(self, basket_address, **kwargs):
        """Handles set up of basket. Calls validation.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        **file_system: fsspec object (optional, defaults to get_file_system())
            The fsspec filesystem to be used for retrieving and uploading. This
            is only used when basket_address is a path and no Pantry object is
            supplied.
        **pantry: weave.Pantry (required if using UUID address)
            The pantry which the basket uuid is associated with. Only for UUID.
        """

        if "pantry" in kwargs:
            self.file_system = kwargs["pantry"].file_system
        else:
            self.file_system = kwargs.get("file_system", None)
            if self.file_system is None:
                self.file_system = get_file_system()
        try:
            self._set_up_basket_from_path(basket_address)
        except ValueError as error:
            if str(error) != f"Basket does not exist: {self.basket_path}":
                raise error

            if "pantry" not in kwargs:
                raise KeyError(
                    "pantry, required to set up basket from UUID,"
                    "is not in kwargs."
                ) from error
            self.set_up_basket_from_uuid(basket_address, kwargs["pantry"])
        if "zip" in str(type(self.file_system)) and os.name == "nt":
            self.manifest_path = "/".join(
                [self.basket_path, "basket_manifest.json"]
            )
            self.supplement_path = "/".join(
                [self.basket_path, "basket_supplement.json"]
            )
            self.metadata_path = "/".join(
                [self.basket_path, "basket_metadata.json"]
            )
        else:
            self.manifest_path = os.path.join(
                self.basket_path, "basket_manifest.json"
            )
            self.supplement_path = os.path.join(
                self.basket_path, "basket_supplement.json"
            )
            self.metadata_path = os.path.join(
                self.basket_path, "basket_metadata.json"
            )
        self.validate()

    def _set_up_basket_from_path(self, basket_address):
        """Attempts to set up a basket from a filepath.

        Paramters
        ---------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory or the UUID of the basket. In this case it is assumed to
            be a path to the basket directory.
        """

        self.basket_path = os.fspath(basket_address)
        if "zip" in str(type(self.file_system)) and os.name == "nt":
            self.basket_path = Path(basket_address).as_posix()
        self.validate_basket_path()

    def set_up_basket_from_uuid(self, basket_address, pantry):
        """Attempts to set up a basket from a uuid.

        Note that if the basket cannot be set up from a uuid then an attempt to
        set up the basket from a filepath will be made.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory or the UUID of the basket. In this case it is assumed to
            be the UUID of the basket.
        pantry: weave.Pantry
            The pantry which the basket uuid is associated with.
        """

        try:
            row = pantry.index.get_rows(basket_address)
            pantry.validate_path_in_pantry(row.iloc[0].address)
            self._set_up_basket_from_path(basket_address=row.iloc[0].address)
        except BaseException as error:
            self.basket_path = basket_address

            pantry_error_msg = "Attempting to access basket outside of pantry:"
            if str(error).startswith(pantry_error_msg):
                raise error
            self.validate_basket_path()
            # The above line should raise an exception
            # The below line is more or less a fail safe and will raise the
            # exception
            raise error

    def validate_basket_path(self):
        """Validates basket exists."""
        if not self.file_system.exists(self.basket_path):
            raise ValueError(f"Basket does not exist: {self.basket_path}")

    def validate(self):
        """Validates basket health."""
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

    def __init__(self, basket_address, **kwargs):
        """Initializes the Basket_Class.

        If basket_address is a path, the basket will be loaded directly using
        the file_system or pantry. If basket_address is a UUID, the basket will
        be loaded using the provided pantry's index.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the Basket
            directory or the UUID of the basket.
        **file_system: fsspec object (optional)
            The fsspec filesystem to be used for retrieving and uploading. May
            be used if basket_address is a path.
        **pantry: weave.Pantry (optional)
            The pantry which the basket uuid is associated with. Required if
            basket_address is a UUID.
        """

        super().__init__(basket_address, **kwargs)

        self.manifest = None
        self.supplement = None
        self.metadata = None
        self.get_manifest()
        self.populate_members()

    def populate_members(self):
        """Populate the Basket_Class member variables."""
        self.uuid = self.manifest["uuid"]
        self.upload_time = self.manifest["upload_time"]
        self.parent_uuids = self.manifest["parent_uuids"]
        self.basket_type = self.manifest["basket_type"]
        self.label = self.manifest["label"]
        self.weave_version = self.manifest.get("weave_version", "<0.13.0")
        self.address = self.basket_path
        self.storage_type = self.file_system.__class__.__name__

    def get_manifest(self):
        """Return basket_manifest.json as a python dictionary."""
        if self.manifest is not None:
            return self.manifest

        with self.file_system.open(self.manifest_path, "rb") as file:
            self.manifest = json.load(file)
            return self.manifest

    def get_supplement(self):
        """Return basket_supplement.json as a python dictionary."""
        if self.supplement is not None:
            return self.supplement

        with self.file_system.open(self.supplement_path, "rb") as file:
            self.supplement = json.load(file)
            return self.supplement

    def get_metadata(self):
        """Return basket_metadata.json as a python dictionary.

        Return None if metadata doesn't exist.
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
           within folders.

           Example: if there exists a folder with the
           name 'folder1' within the basket, 'folder1' can be passed
           as the relative path to get back the filesystem.ls results
           of 'folder1'.

        Parameters
        ----------
        relative_path: str (default=None)
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

    # pylint: disable-msg=duplicate-code
    def to_pandas_df(self):
        """Return a dataframe of the basket member variables."""
        data = [
            self.uuid,
            self.upload_time,
            self.parent_uuids,
            self.basket_type,
            self.label,
            self.weave_version,
            self.address,
            self.storage_type,
        ]
        columns = [
            "uuid",
            "upload_time",
            "parent_uuids",
            "basket_type",
            "label",
            "weave_version",
            "address",
            "storage_type",
        ]

        return pd.DataFrame(data=[data], columns=columns)

#Disabling pylint to keep basket in place to a single function for clarity.
# pylint: disable-msg=too-many-locals
def create_basket_in_place(directory_path, **kwargs):
    """Creates a basket in place.
    
    Generates the manifest, supplement, and metadata files directly in the
    provided directory without moving or uploading any files.

    Parameters:
    ----------
    directory_path: string
        The path to the directory where the basket will be created.
    **metadata: dict (optional)
        A dictionary containing metadata to be included in the basket.
    **pantry: object (optional)
        An optional pantry object to which the basket will be added.
    **file_system: fsspec.AbstractFileSystem (optional)
        The file system object. Defaults to S3 file system.
    **parent_uuids: list (optional)
        A list of parent UUIDs for the basket.
    **basket_type: str (optional)
        The type of the basket.
    **label: str (optional)
        The label for the basket.
    **skip_validation: bool (optional)
        Force the create basket function to skip validation.

    Returns:
    ----------
    pd.DataFrame: A single row DataFrame containing the basket information.
    """
    metadata = kwargs.get("metadata", None)
    pantry = kwargs.get("pantry", None)
    file_system = kwargs.get("file_system", None)
    parent_uuids = kwargs.get("parent_uuids", None)
    basket_type = kwargs.get("basket_type", "item")
    label = kwargs.get("label", "")
    skip_validation = kwargs.get("skip_validation", False)

    if file_system is None:
        file_system = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
        )
    if not skip_validation:
        # Validate the directory
        if not validate_basket_in_place_directory(file_system, directory_path):
            raise ValueError(
                "Provided directory cannot be a valid basket "
                "(e.g., no nested baskets allowed)"
            )
        if not validate_basket_in_place_directory_backward(file_system,
                                                           directory_path):
            raise ValueError(
                "Provided directory cannot be a valid basket "
                "(e.g., no nested baskets allowed)"
            )

    # Create manifest file
    manifest = {
        "uuid": str(uuid.uuid1().hex),
        "upload_time": datetime.now(timezone.utc).isoformat(),
        "parent_uuids": parent_uuids or [],
        "basket_type": basket_type,
        "label": label,
    }

    manifest_path = os.path.join(directory_path, "basket_manifest.json")
    with file_system.open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)

    # Create supplement file
    basket_supplement = {"upload_items": [], "integrity_data": []}

    for root, _, files in file_system.walk(directory_path):
        for file in files:
            if file in ["basket_manifest.json",
                        "basket_supplement.json",
                        "basket_metadata.json"]:
                continue

            file_path = os.path.join(root, file)
            integrity_data = derive_integrity_data(
                file_path=file_path, source_file_system=file_system
            )

            basket_supplement["upload_items"].append(
                {"path": file_path, "stub": False}
            )
            basket_supplement["integrity_data"].append(integrity_data)

    supplement_path = os.path.join(directory_path, "basket_supplement.json")
    with file_system.open(supplement_path, "w", encoding="utf-8") as f:
        json.dump(basket_supplement, f, indent=4)

    # Create metadata file if provided
    if metadata:
        metadata_path = os.path.join(directory_path, "basket_metadata.json")
        with file_system.open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)

    # Create the index row as a DataFrame
    index_data = {
        "uuid": [manifest["uuid"]],
        "upload_time": [manifest["upload_time"]],
        "parent_uuids": [manifest["parent_uuids"]],
        "basket_type": [manifest["basket_type"]],
        "label": [manifest["label"]],
        "weave_version": [importlib.metadata.version("weave-db")],
        "address": [directory_path],
        "storage_type": [
            file_system.__class__.__name__
        ],
    }

    single_index_row = pd.DataFrame(index_data)
    # Add to pantry if provided
    if pantry:
        pantry.index.track_basket(single_index_row)

    return single_index_row
