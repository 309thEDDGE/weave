"""Contains functions and classes used by uploader.py's upload function.
"""
import json
import os
import warnings

import jsonschema
from jsonschema import validate

from weave import Index
from .config import manifest_schema, supplement_schema


def validate_pantry(pantry_name, file_system):
    """Starts the validation process off based off the name of the pantry

    Validates that the pantry actually exists at the location given.
    If there is a pantry that exists, check it and every subdirectory by
    calling check_level

    Parameters
    ----------
    pantry_name: str
        the name of the pantry being validated
    file_system: fsspec object
        the file system (s3fs, local fs, etc.) of the pantry
        to validate

    Returns
    ----------
    A list of all invalid basket locations (will return an empty list if
    no warnings are raised)
    """

    if not file_system.exists(pantry_name):
        raise ValueError(
            f"Invalid pantry Path. Pantry does not exist at: {pantry_name}"
        )

    # Catching the warnings that are shown from calling
    # generate_index() to prevent showing the same warning twice
    ind = Index(pantry_name=pantry_name, file_system=file_system)
    with warnings.catch_warnings(record=True):
        try:
            ind.generate_index()
        except json.decoder.JSONDecodeError as error:
            raise ValueError(
                f"Pantry could not be loaded into index: {error}"
            ) from error
    index_df = ind.to_pandas_df()

    # Call check level using the pantry_name as the path
    with warnings.catch_warnings(record=True) as warn:
        _check_level(pantry_name, file_system=file_system, index_df=index_df)
        # Iterate through warn and return the list of warning messages.
        # Enumerate does not work here. prefer to use range and len
        # pylint: disable-next=consider-using-enumerate
        warning_list = [warn[i].message for i in range(len(warn))]
        return warning_list


def _check_level(current_dir, **kwargs):
    """Check all immediate subdirs in dir, check for manifest

    Checks all the immediate subdirectories and files in the given directory
    to see if there is a basket_manifest.json file. If there is a manifest,
    it's a basket and must be validated.
    If there is a directory found, then recursively call check_level with that
    found directory to find if there is a basket in any directory

    Parameters
    ----------
    current_dir: str
        the current directory being searched

    
    **file_system: fsspec object
        the file system (s3fs, local fs, etc.) of the directory being searched
    **index_df: dataframe
        a dataframe representing the index
    **in_basket: bool
        optional parameter. This is a flag to signify that the directory
        is a basket and to search for nested baskets

    Returns
    ----------
    bool that comes from:
        a validate_basket() if there is a basket found
        a check_level() if there is a directory found
        a true if a manifest is found while inside another basket
        a default true if no basket is found
    """
    # Collect kwargs
    file_system = kwargs.get("file_system")
    index_df = kwargs.get("index_df")
    in_basket = kwargs.get("in_basket", False)

    if not file_system.exists(current_dir):
        raise ValueError(
            f"Invalid Path. No file or directory found at: {current_dir}"
        )


    manifest_path = os.path.join(current_dir, 'basket_manifest.json')

    # If a manifest exists, its a basket, validate it
    if file_system.exists(manifest_path):
        # If there is another manifest inside a basket,
        # the nested basket does not need to be validated
        if in_basket:
            return True
        return _validate_basket(current_dir, file_system, index_df)

    # Go through all the other files, if it's a directory, check it
    dirs_and_files = file_system.ls(path=current_dir, refresh=True)

    for file_or_dir in dirs_and_files:
        file_type = file_system.info(file_or_dir)['type']

        if file_type == 'directory':
            # If directory is a basket, check everything under it,
            # for a manifest and return true,
            # this will return true to the _validate_basket
            # and throw an error or warning
            if in_basket:
                return _check_level(file_or_dir,
                                    file_system=file_system,
                                    index_df=index_df,
                                    in_basket=in_basket)
            # If directory is not a basket, check all files in the
            # current dir. If everything is valid, _check_level returns true
            # If it isn't valid, return false
            # and continue looking at all the sub-directories
            if not _check_level(file_or_dir,
                                file_system=file_system,
                                index_df=index_df,
                                in_basket=in_basket):
                return False

    # This is the default backup return.
    # Return True if directory is not a basket because
    # it is valid to have no baskets
    # Return False if directory is a basket because
    # check_level did not find another basket
    return not in_basket


def _validate_basket(basket_dir, file_system, index_df):
    """Takes the root directory of a basket and validates it

    Validation means there is a required basket_manifest.json and
    basket_supplment.json. And an optional basket_metadata.json
    The manifest and supplement are required to follow a certain schema,
    which is defined in config.py.
    All three, manifest, supplement, and metadata are also verified by being
    able to be read into a python dictionary

    If there are any directories found inside this basket, run check_basket()
    on them to see if there is another basket inside this basket. If there is
    another basket, raise an error or warning for invalid pantry.

    If the basket is ever invalid, raise an error warning
    If the basket is valid return true

    Parameters
    ----------
    basket_dir: str
        the path in the file system to the basket root directory
    file_system: fsspec object
        the fsspec file system hosting the pantry to be indexed
    index_df: dataframe
        a dataframe representing the index

    Returns
    ----------
    boolean that is true when the Basket is valid
        if the Basket is invalid, raise a warning
    """
    manifest_path = os.path.join(basket_dir, 'basket_manifest.json')
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')

    # A valid basket has both manifest and supplement
    # If for some reason the manifest is gone,
    # either the directory is wrong,
    # or this function is incorrectly called,
    # or this isn't a basket.
    if not file_system.exists(manifest_path):
        raise FileNotFoundError(f"Invalid Path. "
                                f"No Basket found at: {basket_dir}")

    if not file_system.exists(supplement_path):
        warnings.warn(UserWarning(
            "Invalid Basket. No Supplement file found at: ", basket_dir))

    files_in_basket = file_system.ls(path=basket_dir, refresh=True)

    for file in files_in_basket:
        _, file_name = os.path.split(file)
        {
            "basket_manifest.json": _handle_manifest,
            "basket_supplement.json": _handle_supplement,
            "basket_metadata.json": _handle_metadata
        }.get(
            file_name, _handle_none_of_the_above
        )(file, file_system, index_df)

    # Default return true if there are no problems with this basket
    return True


def _handle_manifest(file, file_system, index_df):
    """Handles case if manifest

    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec object
        The file system to use.
    index_df: dataframe
        a dataframe representing the index
    """
    try:
        # Make sure it can be loaded, valid schema, and valid parent_uuids
        data = json.load(file_system.open(file))
        validate(instance=data, schema=manifest_schema)
        _validate_parent_uuids(data, file_system, index_df)

    except jsonschema.exceptions.ValidationError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Manifest Schema does not match at: ", file
        ))

    except json.decoder.JSONDecodeError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Manifest could not be loaded into json at: ", file
        ))


def _handle_supplement(file, file_system, _index_df):
    """Handles case if supplement

    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec object
        The file system to use.
    index_df: dataframe
        a dataframe representing the index (Currently Unused)
    """
    try:
        # These two lines make sure it can be read and is valid schema
        data = json.load(file_system.open(file))
        validate(instance=data, schema=supplement_schema)

    except jsonschema.exceptions.ValidationError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Supplement Schema does not match at: ", file
        ))

    except json.decoder.JSONDecodeError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Supplement could not be loaded into json at: ", file
        ))


def _handle_metadata(file, file_system, _index_df):
    """Handles case if metadata

    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
        The file system to use.
    index_df: dataframe
        a dataframe representing the index (Currently Unused)
    """
    try:
        json.load(file_system.open(file))

    except json.decoder.JSONDecodeError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Metadata could not be loaded into json at: ", file
        ))


def _handle_none_of_the_above(file, file_system, index_df):
    """Handles case if none of the above

    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec object
        The file system to use.
    index_df: dataframe
        a dataframe representing the index
    """
    basket_dir, _ = os.path.split(file)
    if file_system.info(file)['type'] == 'directory':
        if _check_level(file,
                        file_system=file_system,
                        index_df=index_df,
                        in_basket=True):
            warnings.warn(UserWarning(
                "Invalid Basket. Manifest File "
                "found in sub directory of basket at: ", basket_dir
            ))


def _validate_parent_uuids(data, _file_system, index_df):
    """Validate that all the parent_uuids from the manifest exist in the pantry

    If there are parent uuids that don't actually exist in the pantry,
    a warning will be raised for each of those, along with the
    basket's uuid where the error occurred.

    Parameters
    ----------
    data: dict
        the dictionary that contains the data of the manifest.json
    file_system: fsspec object
        The file system to use. (Currently unused)
    index_df: dataframe
        a dataframe representing the index
    """
    # If there are no parent uuids in the manifest, no need to check anything
    if len(data["parent_uuids"]) == 0:
        return

    man_parent_uids = data["parent_uuids"]

    index_uuids = index_df["uuid"].to_numpy()

    missing_uids = [uid for uid in man_parent_uids if uid not in index_uuids]

    if missing_uids:
        warnings.warn(f"The uuids: {missing_uids} were not found in the "
                      f"index, which was found inside basket: {data['uuid']}")
