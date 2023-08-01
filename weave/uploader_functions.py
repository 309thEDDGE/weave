import json
import os
import hashlib
import math
import tempfile
from datetime import datetime
from pathlib import Path
from weave import config


def validate_upload_item(upload_item):
    """Validates an upload_item"""
    if not isinstance(upload_item, dict):
        raise TypeError(
            "'upload_item' must be a dictionary: "
            f"'upload_item = {upload_item}'"
        )

    expected_schema = {"path": str, "stub": bool}
    for key, value in upload_item.items():
        if key not in expected_schema.keys():
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
    """
    Derive basic integrity data from a file.

    This function takes in a file path and calculates
    the file checksum, file size, and access date (current time).

    Parameters
    ----------
    file_path : str
        Path to file from which integrity data will be derived
    byte_count: int
        If the file size is greater than 3 * byte_count, the checksum
        will be calculated from the beginning, middle, and end bytes
        of the file. For example: If the file size is 10 bytes long
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

    if not byte_count > 0:
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

    # TODO: Read in small chunks of the file at a
    #       time to protect from RAM overload
    if file_size <= byte_count * 3:
        sha256_hash = hashlib.sha256(open(file_path, "rb").read()).hexdigest()
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
        "access_date": datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
        "source_path": file_path,
        "byte_count": byte_count,
    }


class UploadBasket:
    """This class abstracts functionality used by upload_basket."""

    def __init__(
        self,
        upload_items,
        upload_directory,
        file_system,
        unique_id,
        basket_type,
        parent_ids,
        metadata,
        label,
        **kwargs,
    ):
        """Initializes the Basket_Class.

        Parameters
        ----------
        upload_items : [dict]
            List of python dictionaries with the following schema:
            {
                'path': path to the file or folder being uploaded (string),
                'stub': true/false (bool)
            }
            'path' can be a file or folder to be uploaded. Every filename and
            folder name must be unique. If 'stub' is set to True, integrity
            data will be included without uploading the actual file or folder.
            Stubs are useful when original file source information is desired
            without uploading the data itself. This is especially useful when
            dealing with large files.
        upload_directory: str
            Path where basket is to be uploaded (on the upload FS).
        file_system: fsspec object
            The file system to upload to (ie s3fs, local fs, etc).
            If None it will use the default fs from the config.
        unique_id: str
            Unique ID to identify the basket once uploaded.
        basket_type: str
            Type of basket being uploaded.
        parent_ids: optional [str]
            List of unique ids associated with the parent baskets
            used to derive the new basket being uploaded.
        metadata: optional dict,
            Python dictionary that will be written to metadata.json
            and stored in the basket in the upload FS.
        label: optional str,
            Optional user friendly label associated with the basket.
        """
        self.upload_items = upload_items
        self.upload_directory = upload_directory
        self.fs = file_system
        self.unique_id = unique_id
        self.basket_type = basket_type
        self.parent_ids = parent_ids
        self.metadata = metadata
        self.label = label
        self.kwargs = kwargs

    def sanitize_upload_basket_kwargs(self):
        """Sanitizes kwargs for upload_basket"""
        kwargs_schema = {"test_clean_up": bool}
        for key, value in self.kwargs.items():
            if key not in kwargs_schema.keys():
                raise KeyError(f"Invalid kwargs argument: '{key}'")
            if not isinstance(value, kwargs_schema[key]):
                raise TypeError(
                    f"Invalid datatype: '{key}: "
                    f"must be type {kwargs_schema[key]}'"
                )
        self.test_clean_up = self.kwargs.get("test_clean_up", False)

    def sanitize_upload_basket_non_kwargs(self):
        """Sanitize upload_basket's non kwargs args"""
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
            if local_path_basename in config.prohibited_filenames:
                raise ValueError(
                    f"'{local_path_basename}' " "filename not allowed"
                )
            # Check for duplicate file/folder names
            if local_path_basename in local_path_basenames:
                raise ValueError(
                    f"'upload_item' folder and file names must be unique:"
                    f" Duplicate Name = {local_path_basename}"
                )
            else:
                local_path_basenames.append(local_path_basename)

        if not isinstance(self.upload_directory, str):
            raise TypeError(
                "'upload_directory' must be a string: "
                f"'{self.upload_directory}'"
            )
        if not isinstance(self.unique_id, str):
            raise TypeError(
                f"'unique_id' must be a string: '{self.unique_id}'"
            )

        if not isinstance(self.basket_type, str):
            raise TypeError(
                f"'basket_type' must be a string: " f"'{self.basket_type}'"
            )

        if not (
            isinstance(self.parent_ids, list)
            and all(isinstance(x, str) for x in self.parent_ids)
        ):
            raise TypeError(
                f"'parent_ids' must be a list of strings: '{self.parent_ids}'"
            )

        if not isinstance(self.metadata, dict):
            raise TypeError(
                "'metadata' must be a dictionary: " f"'{self.metadata}'"
            )

        if not isinstance(self.label, str):
            raise TypeError(f"'label' must be a string: '{self.label}'")

    def sanitize_args(self):
        """Sanitize all args with one call, that's all"""
        self.sanitize_upload_basket_kwargs()
        self.sanitize_upload_basket_non_kwargs()

    def check_that_upload_dir_does_not_exist(self):
        """Ensure that upload directory does not previously exist

        This averts some errors with uploading to a directory that already
        exists
        """
        if self.fs.isdir(self.upload_directory):
            raise FileExistsError(
                "'upload_directory' already exists: "
                f"'{self.upload_directory}''"
            )

    def setup_temp_dir_for_staging_prior_to_fs(self):
        """Sets up a temporary directory to hold stuff before upload to FS"""
        self.upload_path = f"{self.upload_directory}"
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fs.mkdir(self.upload_path)
        self.temp_dir_path = self.temp_dir.name

    def upload_files_and_stubs_to_fs(self):
        """Method to upload both files and stubs to fs"""
        supplement_data = {}
        supplement_data["upload_items"] = self.upload_items
        supplement_data["integrity_data"] = []

        for upload_item in self.upload_items:
            upload_item_path = Path(upload_item["path"])
            if upload_item_path.is_dir():
                for root, dirs, files in os.walk(upload_item_path):
                    for name in files:
                        local_path = os.path.join(root, name)
                        # fid means "file integrity data"
                        fid = derive_integrity_data(str(local_path))
                        if upload_item["stub"] is False:
                            fid["stub"] = False
                            file_upload_path = os.path.join(
                                self.upload_path,
                                os.path.relpath(
                                    local_path,
                                    os.path.split(upload_item_path)[0],
                                ),
                            )
                            fid["upload_path"] = str(file_upload_path)
                            base_path = os.path.split(file_upload_path)[0]
                            if not self.fs.exists(base_path):
                                self.fs.mkdir(base_path)
                            self.fs.upload(local_path, file_upload_path)
                        else:
                            fid["stub"] = True
                            fid["upload_path"] = "stub"
                        supplement_data["integrity_data"].append(fid)
            else:
                fid = derive_integrity_data(str(upload_item_path))
                if upload_item["stub"] is False:
                    fid["stub"] = False
                    file_upload_path = os.path.join(
                        self.upload_path, os.path.basename(upload_item_path)
                    )
                    fid["upload_path"] = str(file_upload_path)
                    base_path = os.path.split(file_upload_path)[0]
                    if not self.fs.exists(base_path):
                        self.fs.mkdir(base_path)
                    self.fs.upload(str(upload_item_path), file_upload_path)
                else:
                    fid["stub"] = True
                    fid["upload_path"] = "stub"
                supplement_data["integrity_data"].append(fid)
        self.supplement_data = supplement_data

    def create_and_upload_basket_json_to_fs(self):
        """Creates and dumps a JSON containing basket metadata"""
        basket_json_path = os.path.join(
            self.temp_dir_path, "basket_manifest.json"
        )
        basket_json = {}
        basket_json["uuid"] = self.unique_id
        basket_json["upload_time"] = datetime.now().strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        basket_json["parent_uuids"] = self.parent_ids
        basket_json["basket_type"] = self.basket_type
        basket_json["label"] = self.label

        with open(basket_json_path, "w") as outfile:
            json.dump(basket_json, outfile)
        self.fs.upload(
            basket_json_path,
            os.path.join(self.upload_path, "basket_manifest.json"),
        )

    def upload_basket_metadata_to_fs(self):
        """Dumps metadata to tempdir, and then uploads to FS"""
        metadata_path = os.path.join(
            self.temp_dir_path, "basket_metadata.json"
        )
        if self.metadata != {}:
            with open(metadata_path, "w") as outfile:
                json.dump(self.metadata, outfile, default=str)
            self.fs.upload(
                metadata_path,
                os.path.join(self.upload_path, "basket_metadata.json"),
            )

    def upload_basket_supplement_to_fs(self):
        """Dumps metadata to tempdir, and then uploads to FS"""
        supplement_json_path = os.path.join(
            self.temp_dir_path, "basket_supplement.json"
        )
        with open(supplement_json_path, "w") as outfile:
            json.dump(self.supplement_data, outfile)
        self.fs.upload(
            supplement_json_path,
            os.path.join(self.upload_path, "basket_supplement.json"),
        )

    def fs_upload_path_exists(self):
        """Returns True if fs upload_path has been created, else False"""
        return self.fs.exists(self.upload_path)

    def clean_out_fs_upload_dir(self):
        """Removes everything from upload_path inside fs"""
        self.fs.rm(self.upload_path, recursive=True)

    def tear_down_temp_dir(self):
        """For use at death of class. Cleans up temp_dir."""
        self.temp_dir.cleanup()

def upload_basket(
    upload_items,
    upload_directory,
    file_system,
    unique_id,
    basket_type,
    parent_ids=[],
    metadata={},
    label="",
    **kwargs
):
    """
    Upload files and directories to the upload FS.

    This function takes in a list of items to upload, along with
    identifying tags, and uploads the data together with three
    json files: basket_manifest.json, basket_metadata.json, and
    supplement.json. The contents of the three files are specified
    below. These three files together with the data specified by
    upload_items are uploaded to the upload_directory path within
    the upload FS as a basket of data.

    basket_manifest.json contains:
        1) unique_id
        2) list of parent ids
        3) basket type
        4) label
        5) upload date

    basket_metadata.json contains:
        1) dictionary passed in through the metadata parameter

    basket_supplement.json contains:
        1) the upload_items object that was passed as an input parameter
        2) integrity_data for every file uploaded
            a) file checksum
            b) upload date
            c) file size (bytes)
            d) source path (original file location)
            e) stub (true/false)
            f) upload path (path where the file is uploaded in the upload FS)

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
    upload_directory: str
        Path where basket is to be uploaded.
    file_system: fsspec object
        The file system to upload to (ie s3fs, local fs, etc).
        If None it will use the default fs from the config.
    unique_id: str
        Unique ID to identify the basket once uploaded.
    basket_type: str
        Type of basket being uploaded.
    parent_ids: optional [str]
        List of unique ids associated with the parent baskets
        used to derive the new basket being uploaded.
    metadata: optional dict,
        Python dictionary that will be written to metadata.json
        and stored in the basket in upload FS.
    label: optional str,
        Optional user friendly label associated with the basket.
    """
    upload_basket_obj = UploadBasket(
        upload_items,
        upload_directory,
        file_system,
        unique_id,
        basket_type,
        parent_ids,
        metadata,
        label,
        **kwargs
    )

    upload_basket_obj.sanitize_args()
    upload_basket_obj.check_that_upload_dir_does_not_exist()

    try:
        upload_basket_obj.setup_temp_dir_for_staging_prior_to_fs()
        upload_basket_obj.upload_files_and_stubs_to_fs()
        upload_basket_obj.create_and_upload_basket_json_to_fs()
        upload_basket_obj.upload_basket_metadata_to_fs()
        upload_basket_obj.upload_basket_supplement_to_fs()

        if upload_basket_obj.test_clean_up:
            raise Exception("Test Clean Up")

    except Exception as e:
        if upload_basket_obj.fs_upload_path_exists():
            upload_basket_obj.clean_out_fs_upload_dir()
        raise e

    finally:
        upload_basket_obj.tear_down_temp_dir()
