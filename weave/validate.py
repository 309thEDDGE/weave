"""Contains functions and classes used by uploader.py's upload function.
"""
import json
import os

import jsonschema
from jsonschema import validate

from .config import manifest_schema, supplement_schema


def validate_bucket(bucket_name, file_system):
    """Starts the validation process off based off the name of the bucket

    Validates that the bucket actually exists at the location given.
    If there is a bucket that exists, check it and every subdirectory by
    calling check_level

    Parameters
    ----------
    bucket_name: string
        the name of the bucket in s3fs

    Returns
    ----------
    A bool of whether the bucket is valid or not that comes from check_level()
    """
    if not file_system.exists(bucket_name):
        raise ValueError(
            f"Invalid Bucket Path. Bucket does not exist at: {bucket_name}"
        )

    # call check level, with a path, but since we're just starting,
    # we just use the bucket_name as the path
    return _check_level(bucket_name, file_system)


def _check_level(current_dir, file_system, in_basket=False):
    """Check all immediate subdirs in dir, check for manifest

    Checks all the immediate subdirectories and files in the given directory 
    to see if there is a basket_manifest.json file. If there is a manifest, 
    it's a basket and must be validated.
    If there is a directory found, then recursively call check_level with that
    found directory to find if there is a basket in any directory

    Parameters
    ----------
    current_dir: string
        the current directory that we want to search all files and 
        directories of
    in_basket: bool
        optional parameter. This is a flag to signify that we are in a basket
        and we are looking for a nested basket now. 

    Returns
    ----------
    bool that comes from:
        a validate_basket() if there is a basket found
        a check_level() if there is a directory found
        a true if we found a manifest while inside another basket
        a default true if no basket is found
    """
    if not file_system.exists(current_dir):
        raise ValueError(
            f"Invalid Path. No file or directory found at: {current_dir}"
        )

    manifest_path = os.path.join(current_dir, 'basket_manifest.json')

    # if a manifest exists, its a basket, validate it
    if file_system.exists(manifest_path):
        # if we find another manifest inside a basket, we just need to say
        # we found it, we don't need to validate the nested basket
        if in_basket:
            return True
        return _validate_basket(current_dir, file_system)

    # go through all the other files, if it's a directory, we need to check it
    dirs_and_files = file_system.find(
        path=current_dir,
        maxdepth=1,
        withdirs=True
    )

    for file_or_dir in dirs_and_files:
        file_type = file_system.info(file_or_dir)['type']

        if file_type == 'directory':
            # if we are in a basket, check evrything under it, for a manifest
            # and return true, this will return true to the _validate_basket
            # and throw an error
            if in_basket:
                return _check_level(file_or_dir, file_system,
                                    in_basket=in_basket)
            # if we aren't in the basket, we want to check all files in our
            # current dir. If everything is valid, _check_level returns true
            # if it isn't valid, we go in and return false
            # we don't want to return _check_level because we want to keep
            # looking at all the sub-directories
            if not _check_level(file_or_dir, file_system,
                                in_basket=in_basket):
                return False

    # This is the default backup return.
    # If we are in a basket, it will be valid if we return false,
    # because we want to signify that we didn't find another basket
    # If we are not in a basket, we want to return true, because
    # we didn't find a basekt and it was valid to have no baskets
    return not in_basket


def _validate_basket(basket_dir, file_system):
    """Takes the root directory of a basket and validates it

    Validation means there is a required basket_manifest.json and
    basket_supplment.json. And an optional basket_metadata.json
    The manifest and supplement are required to follow a certain schema,
    which is defined in config.py.
    All three, manifest, supplement, and metadata are also verified by being 
    able to be read into a python dictionary

    If there are any directories found inside this basket, run check_basket()
    on them to see if there is another basket inside this basket. If there is
    another basket, raise error for invalid bucket.

    If the Basket is ever invalid, raise an error
    If the basket is valid return true

    Parameters
    ----------
    basket_dir: string
        The path in the file system to the basket root directory
    file_system: fsspec object
        The fsspec file system hosting the bucket to be indexed.

    Returns
    -------
    boolean that is true when the Basket is valid
        if the Basket is invalid, raise an error
    """
    manifest_path = os.path.join(basket_dir, 'basket_manifest.json')
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')

    # a valid basket has both manifest and supplement
    # if for some reason the manifest is gone, we get a wrong directory,
    # or this function is incorrectly called,
    # we can say that this isn't a basket.
    if not file_system.exists(manifest_path):
        raise FileNotFoundError(
            f"Invalid Path. No Basket found at: {basket_dir}"
        )

    if not file_system.exists(supplement_path):
        raise FileNotFoundError(
            "Invalid Basket. No Supplement file found at: ", basket_dir
        )

    files_in_basket = file_system.find(
        path=basket_dir,
        maxdepth=1,
        withdirs=True
    )

    for file in files_in_basket:
        _, file_name = os.path.split(file)
        {
            "basket_manifest.json": handle_manifest,
            "basket_supplement.json": handle_supplement,
            "basket_metadata.json": handle_metadata
        }.get(file_name, handle_none_of_the_above)(file, file_system)

    # default return true if we don't find any problems with this basket
    return True


def handle_manifest(file, file_system):
    """Handles case if manifest
    
    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
        The file system to use.
    """
    try:
        # these two lines make sure it can be read and is valid schema
        data = json.load(file_system.open(file))
        validate(instance=data, schema=manifest_schema)

    except jsonschema.exceptions.ValidationError as exc:
        raise ValueError(
            "Invalid Basket. Manifest Schema does not match at: ", file
        ) from exc

    except json.decoder.JSONDecodeError as exc:
        raise ValueError(
            "Invalid Basket. Manifest could not be loaded into json at: ", file
        ) from exc


def handle_supplement(file, file_system):
    """Handles case if supplement
    
    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
        The file system to use.
    """
    try:
        # these two lines make sure it can be read and is valid schema
        data = json.load(file_system.open(file))
        validate(instance=data, schema=supplement_schema)

    except jsonschema.exceptions.ValidationError as exc:
        raise ValueError(
            "Invalid Basket. "
            "Supplement Schema does not match at: ", file
        ) from exc

    except json.decoder.JSONDecodeError as exc:
        raise ValueError(
            "Invalid Basket. "
            "Supplement could not be loaded into json at: ", file
        ) from exc

def handle_metadata(file, file_system):
    """Handles case if metadata
    
    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
        The file system to use.
    """
    try:
        json.load(file_system.open(file))

    except json.decoder.JSONDecodeError as exc:
        raise ValueError(
            "Invalid Basket. "
            "Metadata could not be loaded into json at: ", file
        ) from exc


def handle_none_of_the_above(file, file_system):
    """Handles case if none of the above
    
    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
        The file system to use.
    """
    basket_dir, _ = os.path.split(file)
    if file_system.info(file)['type'] == 'directory':
        if _check_level(file, file_system, in_basket=True):
            raise ValueError("Invalid Basket. Manifest File "
                             "found in sub directory of basket at: ",
                             basket_dir
            )
