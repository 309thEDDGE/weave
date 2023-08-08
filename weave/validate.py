import json
import os
import warnings

import jsonschema
from jsonschema import validate

from weave import config


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

    fs = file_system

    if not fs.exists(bucket_name):
        raise ValueError(
            f"Invalid Bucket Path. Bucket does not exist at: {bucket_name}"
        )

    # call check level, with a path, but since we're just starting,
    # we just use the bucket_name as the path
    with warnings.catch_warnings(record=True) as w:
        _check_level(bucket_name, fs)
    if len(w) > 0:
        return w
    return True


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

    fs = file_system

    if not fs.exists(current_dir):
        raise ValueError(
            f"Invalid Path. No file or directory found at: {current_dir}"
        )

    manifest_path = os.path.join(current_dir, 'basket_manifest.json')

    # if a manifest exists, its a basket, validate it
    if fs.exists(manifest_path):
        # if we find another manifest inside a basket, we just need to say
        # we found it, we don't need to validate the nested basket
        if in_basket:
            return True
        return _validate_basket(current_dir, fs)

    # go through all the other files, if it's a directory, we need to check it
    dirs_and_files = fs.find(
        path=current_dir,
        maxdepth=1,
        withdirs=True
    )

    for file_or_dir in dirs_and_files:
        file_type = fs.info(file_or_dir)['type']

        if file_type == 'directory':
            # if we are in a basket, check evrything under it, for a manifest
            # and return true, this will return true to the _validate_basket
            # and throw an error
            if in_basket:
                return _check_level(file_or_dir, fs, in_basket=in_basket)
            # if we aren't in the basket, we want to check all files in our
            # current dir. If everything is valid, _check_level returns true
            # if it isn't valid, we go in and return false
            # we don't want to return _check_level because we want to keep
            # looking at all the sub-directories
            else:
                if not _check_level(file_or_dir, fs, in_basket=in_basket):
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
        the path in s3fs to the basket root directory

    Returns
    ----------
    boolean that is true when the Basket is valid
        if the Basket is invalid, raise an error
    """

    fs = file_system

    manifest_path = os.path.join(basket_dir, 'basket_manifest.json')
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')

    # a valid basket has both manifest and supplement
    # if for some reason the manifest is gone, we get a wrong directory,
    # or this function is incorrectly called, 
    # we can say that this isn't a basket. 
    if not fs.exists(manifest_path):
        warnings.warn(f"Invalid Path. No Basket found at: {basket_dir}")

    if not fs.exists(supplement_path):
        warnings.warn(
            f"Invalid Basket. No Supplement file found at: {basket_dir}")

    files_in_basket = fs.find(
        path=basket_dir,
        maxdepth=1,
        withdirs=True
    )

    for file in files_in_basket:
        basket_dir, file_name = os.path.split(file)

        if file_name == 'basket_manifest.json':
            try:
                # these two lines make sure it can be read and is valid schema
                data = json.load(fs.open(file))
                validate(instance=data, schema=config.manifest_schema)

            except jsonschema.exceptions.ValidationError:
                warnings.warn(
                    f"Invalid Basket. "
                    f"Manifest Schema does not match at: {file}"
                )

            except json.decoder.JSONDecodeError:
                warnings.warn(
                    f"Invalid Basket. "
                    f"Manifest could not be loaded into json at: {file}"
                )

        if file_name == 'basket_supplement.json':
            try:
                # these two lines make sure it can be read and is valid schema
                data = json.load(fs.open(file))
                validate(instance=data, schema=config.supplement_schema)

            except jsonschema.exceptions.ValidationError:
                warnings.warn(
                    f"Invalid Basket. "
                    f"Supplement Schema does not match at: {file}"
                )

            except json.decoder.JSONDecodeError:
                warnings.warn(
                    f"Invalid Basket. "
                    f"Supplement could not be loaded into json at: {file}"
                )

        if file_name == 'basket_metadata.json':
            try:
                data = json.load(fs.open(file))

            except json.decoder.JSONDecodeError:
                warnings.warn(
                    f"Invalid Basket. "
                    f"Metadata could not be loaded into json at: {file}"
                )

        # if we find a directory inside this basket, we need to check it
        # if we check it and find a basket, this basket is invalid.
        if fs.info(file)['type'] == 'directory':
            if _check_level(file, fs, in_basket=True):
                warnings.warn(f"Invalid Basket. Manifest File "
                              f"found in sub directory of basket at: "
                              f"{basket_dir}"
                )

    # default return true if we don't find any problems with this basket
    return True