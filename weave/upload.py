"""Contains functions and classes concerning the upload functionality."""

import hashlib
from importlib import metadata
import json
import math
import os
import tempfile
import uuid
from datetime import datetime, timezone as tz
from pathlib import Path

from .config import get_file_system, prohibited_filenames


def validate_upload_item(upload_item):
    """Validates an upload_item."""
    if not isinstance(upload_item, dict):
        raise TypeError(
            "'upload_item' must be a dictionary: "
            f"'upload_item = {upload_item}'"
        )

    expected_schema = {"path": str, "stub": bool}
    for key, value in upload_item.items():
        if key not in expected_schema:
            raise KeyError(
                f"Invalid upload_item key: '{key}'"
                f"\nExpected keys: {list(expected_schema.keys())}"
            )
        if not isinstance(value, expected_schema[key]):
            raise TypeError(
                f"Invalid upload_item type: '{key}: {type(value)}'"
                f"\nExpected type: {expected_schema[key]}"
            )

    if not os.path.exists(upload_item["path"]):
        raise FileExistsError(
            f"'path' does not exist: '{upload_item['path']}'"
        )


def derive_integrity_data(file_path, byte_count=10**8):
    """Derives basic integrity data from a file.

    This function takes in a file path and calculates
    the file checksum, file size, and access date (current time).

    Parameters
    ----------
    file_path : str
        Path to file from which integrity data will be derived.
    byte_count: int (default=10**8)
        If the file size is greater than 3 * byte_count, the checksum
        will be calculated from the beginning, middle, and end bytes
        of the file.
        For example: If the file size is 10 bytes long
        and the byte_count is 2, the checksum will be calculated from bytes
        1, 2 (beginning two bytes), 5, 6 (middle two bytes) and 9, 10
        (last two bytes). This option is provided to speed up checksum
        calculation for large files.

    Returns
    ----------
    Dictionary
     {
      'file_size': bytes (int),
      'hash': sha256 hash (string),
      'access_date': current date/time (string)
      'source_path': path to the original source of data (string)
      'byte_count': byte count used for generated checksum (int)
     }
    """

    if not isinstance(file_path, str):
        raise TypeError(f"'file_path' must be a string: '{file_path}'")

    if not os.path.isfile(file_path):
        raise FileExistsError(f"'file_path' does not exist: '{file_path}'")

    if not isinstance(byte_count, int):
        raise TypeError(f"'byte_count' must be an int: '{byte_count}'")

    if byte_count <= 0:
        raise ValueError(
            f"'byte_count' must be greater than zero: '{byte_count}'"
        )

    max_byte_count = 300 * 10**6
    if byte_count > max_byte_count:
        raise ValueError(
            f"'byte_count' must be less than or equal to {max_byte_count}"
            f" bytes: '{byte_count}'"
        )

    file_size = os.path.getsize(file_path)

    if file_size <= byte_count * 3:
        with open(file_path, "rb") as file:
            sha256_hash = hashlib.sha256(file.read()).hexdigest()
    else:
        hasher = hashlib.sha256()
        midpoint = file_size / 2.0
        midpoint_seek_position = math.floor(midpoint - byte_count / 2.0)
        end_seek_position = file_size - byte_count
        with open(file_path, "rb") as file:
            hasher.update(file.read(byte_count))
            file.seek(midpoint_seek_position)
            hasher.update(file.read(byte_count))
            file.seek(end_seek_position)
            hasher.update(file.read(byte_count))
        sha256_hash = hasher.hexdigest()

    return {
        "file_size": file_size,
        "hash": sha256_hash,
        "access_date": datetime.now(tz.utc).isoformat(),
        "source_path": file_path,
        "byte_count": byte_count,
    }


class UploadBasket:
    """This class abstracts functionality used by upload_basket."""
    def __init__(
        self,
        upload_items,
        **kwargs,
    ):
        """Initializes the Basket_Class.

        Parameters
        ----------
        upload_items : [dict]
            List of python dictionaries with the following schema:
            {
                'path': path to the file or folder being uploaded (str),
                'stub': true/false (bool)
            }
            'path' can be a file or folder to be uploaded. Every filename and
            folder name must be unique. If 'stub' is set to True, integrity
            data will be included without uploading the actual file or folder.
            Stubs are useful when original file source information is desired
            without uploading the data itself. This is especially useful when
            dealing with large files.
        **upload_directory: str (required)
            Path where basket is to be uploaded (on the upload FS).
        **unique_id: str (required)
            Unique ID to identify the basket once uploaded.
        **basket_type: str
            Type of basket being uploaded.
        **parent_ids: [str] (optional)
            List of unique ids associated with the parent baskets
            used to derive the new basket being uploaded.
        **metadata: dict (optional)
            Python dictionary that will be written to metadata.json
            and stored in the basket in the upload FS.
        **label: str (optional)
            Optional user friendly label associated with the basket.
        **file_system: fsspec object (optional)
            The file system to upload to (ie s3fs, local fs, etc).
            If None, it will use the default fs from the weave.config.
        **pantry_path: str
            Path to the pantry that will hold this basket.

        Please note that either the upload_directory OR the basket_type must
        be provided. IT IS RECOMMENDED that the user simply provide the
        basket_type as this will allow the library to choose a good unique_id,
        and keep the pantry organized.
        """

        self.upload_items = upload_items
        self.kwargs = kwargs
        # We cannot use with in this case, as we want the temp dir to persist
        # beyond this function.
        # pylint: disable-next=consider-using-with
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        self.run_logic()

    def run_logic(self):
        """Handles running the functions that make up the class."""
        self.sanitize_args()
        self.check_that_upload_dir_does_not_exist()

        try:
            self.setup_temp_dir_for_staging_prior_to_fs()
            self.upload_files_and_stubs_to_fs()
            self.create_and_upload_basket_json_to_fs()
            self.upload_basket_metadata_to_fs()
            self.upload_basket_supplement_to_fs()

            if self.kwargs.get("test_clean_up", False):
                class TestException(Exception):
                    """Custom exception for test excepting purposes."""
                raise TestException("Test Clean Up")

        except Exception as the_exception:
            if self.fs_upload_path_exists():
                self.clean_out_fs_upload_dir()
            raise the_exception

    def sanitize_upload_basket_kwargs(self):
        """Sanitizes kwargs for upload_basket."""
        kwargs_schema = {"test_clean_up": bool,
                         "file_system": object,
                         "upload_directory": str,
                         "unique_id": str,
                         "basket_type": str,
                         "parent_ids": list,
                         "metadata": dict,
                         "label": str,
                         "weave_version": str,
                         "pantry_path": str,
                         "test_prefix": str}
        for key, value in self.kwargs.items():
            if key not in kwargs_schema:
                raise KeyError(f"Invalid kwargs argument: '{key}'")
            if not isinstance(value, kwargs_schema[key]):
                raise TypeError(
                    f"Invalid datatype: '{key}: "
                    f"must be type {kwargs_schema[key]}'"
                )
        # parent_ids requires further examination:
        parent_ids = self.kwargs.get("parent_ids", None)
        if (
            (parent_ids is not None)
            and
            not (all(isinstance(x, str) for x in parent_ids))
        ):
            raise TypeError(
                f"'parent_ids' must be a list of strings: '{parent_ids}'"
            )
        # I think it is wise to ignore pylint here because we should only
        # set self.file_system *after* we have sanitized it.
        # pylint: disable-next=attribute-defined-outside-init
        self.file_system = self.kwargs.get("file_system", get_file_system())

    def sanitize_upload_basket_non_kwargs(self):
        """Sanitize upload_basket's non kwargs args."""
        if not isinstance(self.upload_items, list):
            raise TypeError(
                "'upload_items' must be a list of dictionaries: "
                f"'{self.upload_items}'"
            )

        if not all(isinstance(x, dict) for x in self.upload_items):
            raise TypeError(
                "'upload_items' must be a list of dictionaries: "
                f"'{self.upload_items}'"
            )

        # Validate self.upload_items
        local_path_basenames = []
        for upload_item in self.upload_items:
            validate_upload_item(upload_item)
            local_path_basename = os.path.basename(Path(upload_item["path"]))
            if local_path_basename in prohibited_filenames:
                raise ValueError(
                    f"'{local_path_basename}' " "filename not allowed"
                )
            # Check for duplicate file/folder names
            if local_path_basename in local_path_basenames:
                raise ValueError(
                    f"'upload_item' folder and file names must be unique:"
                    f" Duplicate Name = {local_path_basename}"
                )
            local_path_basenames.append(local_path_basename)

    def sanitize_args(self):
        """Sanitize all args with one call."""
        self.sanitize_upload_basket_kwargs()
        self.sanitize_upload_basket_non_kwargs()

    def _get_path(self):
        """Either make sure upload_path is in kwargs or create it."""

        if "unique_id" not in self.kwargs:
            self.kwargs["unique_id"] = uuid.uuid1().hex
        if "upload_directory" not in self.kwargs:
            if not ("pantry_path" and "basket_type" in self.kwargs):
                raise ValueError(
                    "Please provide either the 'upload_directory' or a "
                    "combination of 'pantry_path' and 'basket_type' as kwargs "
                    "for UploadBasket"
                )
            self.kwargs["upload_directory"] = os.path.join(
                self.kwargs.get("test_prefix", ""),
                self.kwargs.get("pantry_path"),
                self.kwargs.get("basket_type"),
                self.kwargs.get("unique_id"),
            )

    def check_that_upload_dir_does_not_exist(self):
        """Ensure that upload directory does not previously exist.

        This averts some errors with uploading to a directory that already
        exists.
        """

        self._get_path()
        upload_directory = self.kwargs.get("upload_directory")
        if self.file_system.isdir(upload_directory):
            raise FileExistsError(
                f"'upload_directory' already exists: '{upload_directory}''"
            )

    def setup_temp_dir_for_staging_prior_to_fs(self):
        """Sets up a temporary directory to hold stuff before upload to FS."""
        self.file_system.mkdir(self.kwargs.get("upload_directory"))

    def upload_files_and_stubs_to_fs(self):
        """Method to upload both files and stubs to FS."""
        supplement_data = {}
        supplement_data["upload_items"] = self.upload_items
        supplement_data["integrity_data"] = []

        # I cannot figure out how to appease pylint on this one:
        # pylint: disable-next=too-many-nested-blocks
        for upload_item in self.upload_items:
            upload_item_path = Path(upload_item["path"])
            if upload_item_path.is_dir():
                for root, _, files in os.walk(upload_item_path):
                    for name in files:
                        local_path = os.path.join(root, name)
                        # fid means "file integrity data"
                        fid = derive_integrity_data(str(local_path))
                        if upload_item["stub"] is False:
                            fid["stub"] = False
                            file_upload_path = os.path.join(
                                self.kwargs.get("upload_directory"),
                                os.path.relpath(
                                    local_path,
                                    os.path.split(upload_item_path)[0],
                                ),
                            )
                            fid["upload_path"] = str(file_upload_path)
                            base_path = os.path.split(file_upload_path)[0]
                            if not self.file_system.exists(base_path):
                                self.file_system.mkdir(base_path)
                            self.file_system.upload(local_path,
                                                    file_upload_path)
                        else:
                            fid["stub"] = True
                            fid["upload_path"] = "stub"
                        supplement_data["integrity_data"].append(fid)
            else:
                fid = derive_integrity_data(str(upload_item_path))
                if upload_item["stub"] is False:
                    fid["stub"] = False
                    file_upload_path = os.path.join(
                        self.kwargs.get("upload_directory"),
                        os.path.basename(upload_item_path)
                    )
                    fid["upload_path"] = str(file_upload_path)
                    base_path = os.path.split(file_upload_path)[0]
                    if not self.file_system.exists(base_path):
                        self.file_system.mkdir(base_path)
                    self.file_system.upload(str(upload_item_path),
                                            file_upload_path)
                else:
                    fid["stub"] = True
                    fid["upload_path"] = "stub"
                supplement_data["integrity_data"].append(fid)
        self.kwargs["supplement_data"] = supplement_data

    def create_and_upload_basket_json_to_fs(self):
        """Creates and dumps a JSON containing basket metadata."""
        basket_json_path = os.path.join(
            self.temp_dir_path, "basket_manifest.json"
        )
        basket_json = {}
        basket_json["uuid"] = self.kwargs.get("unique_id")
        basket_json["upload_time"] = datetime.now(tz.utc).isoformat()
        basket_json["parent_uuids"] = self.kwargs.get("parent_ids", [])
        basket_json["basket_type"] = self.kwargs.get("basket_type")
        basket_json["label"] = self.kwargs.get("label","")
        basket_json["weave_version"] = metadata.version("weave-db")

        with open(basket_json_path, "w", encoding="utf-8") as outfile:
            json.dump(basket_json, outfile)
        self.file_system.upload(
            basket_json_path,
            os.path.join(self.kwargs.get("upload_directory"),
                         "basket_manifest.json"),
        )

    def upload_basket_metadata_to_fs(self):
        """Dumps metadata to tempdir, and then uploads to FS."""
        metadata_path = os.path.join(
            self.temp_dir_path, "basket_metadata.json"
        )
        if "metadata" in self.kwargs:
            with open(metadata_path, "w", encoding="utf-8") as outfile:
                json.dump(self.kwargs.get("metadata"), outfile, default=str)
            self.file_system.upload(
                metadata_path,
                os.path.join(self.kwargs.get("upload_directory"),
                             "basket_metadata.json"),
            )

    def upload_basket_supplement_to_fs(self):
        """Dumps metadata to tempdir, and then uploads to FS."""
        supplement_json_path = os.path.join(
            self.temp_dir_path, "basket_supplement.json"
        )
        with open(supplement_json_path, "w", encoding="utf-8") as outfile:
            json.dump(self.kwargs.get("supplement_data"), outfile)
        self.file_system.upload(
            supplement_json_path,
            os.path.join(self.kwargs.get("upload_directory"),
                         "basket_supplement.json"),
        )

    def fs_upload_path_exists(self):
        """Returns True if FS upload_path has been created, else False."""
        return self.file_system.exists(self.kwargs.get("upload_directory"))

    def clean_out_fs_upload_dir(self):
        """Removes everything from upload_path inside of FS."""
        self.file_system.rm(self.kwargs.get("upload_directory"),
                            recursive=True)

    def get_upload_path(self):
        """Gets upload path from kwargs and returns it."""
        upload_path = self.kwargs.get("upload_directory")
        if upload_path is not None:
            return upload_path
        raise ValueError("Somehow 'upload_path' is not available in kwargs")
