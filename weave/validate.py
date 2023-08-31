"""Contains functions and classes used by uploader.py's upload function.
"""
import json
import os
import warnings

import jsonschema
from jsonschema import validate

from .config import manifest_schema, supplement_schema


def validate_pantry(pantry):
    """Starts the validation process off based off the name of the pantry

    Validates that the pantry actually exists at the location given.
    If there is a pantry that exists, check it and every subdirectory by
    calling check_level

    Parameters
    ----------
    pantry: weave.Pantry
        Pantry to be validated.

    Returns
    ----------
    A list of all invalid basket locations (will return an empty list if
    no warnings are raised)
    """

    if not pantry.file_system.exists(pantry.pantry_path):
        raise ValueError(
            f"Invalid pantry Path. Pantry does not exist at: "
            f"{pantry.pantry_path}"
        )

    # Here we are catching the warnings that are shown from calling
    # generate_index() because we don't want to show the same warning twice
    with warnings.catch_warnings(record=True):
        try:
            pantry.index.generate_index()
        except json.decoder.JSONDecodeError as error:
            raise ValueError(
                f"Pantry could not be loaded into index: {error}"
            ) from error

    # Call check level, with a path, but since we're just starting,
    # We just use the pantry_name as the path
    with warnings.catch_warnings(record=True) as warn:
        _check_level(pantry.pantry_path, pantry=pantry)
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
    current_dir: string
        the current directory that we want to search all files and
        directories of

    kwargs:
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
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
    # Collect kwargs
    pantry = kwargs.get("pantry")
    file_system = pantry.file_system
    in_basket = kwargs.get("in_basket", False)

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
        return _validate_basket(current_dir, pantry)

    # go through all the other files, if it's a directory, we need to check it
    dirs_and_files = file_system.ls(path=current_dir, refresh=True)

    for file_or_dir in dirs_and_files:
        file_type = file_system.info(file_or_dir)['type']

        if file_type == 'directory':
            # if we are in a basket, check evrything under it, for a manifest
            # and return true, this will return true to the _validate_basket
            # and throw an error or warning
            if in_basket:
                return _check_level(file_or_dir,
                                    pantry=pantry,
                                    in_basket=in_basket)
            # if we aren't in the basket, we want to check all files in our
            # current dir. If everything is valid, _check_level returns true
            # if it isn't valid, we go in and return false
            # we don't want to return _check_level because we want to keep
            # looking at all the sub-directories
            if not _check_level(file_or_dir,
                                pantry=pantry,
                                in_basket=in_basket):
                return False

    # This is the default backup return.
    # If we are in a basket, it will be valid if we return false,
    # because we want to signify that we didn't find another basket
    # If we are not in a basket, we want to return true, because
    # we didn't find a basket and it was valid to have no baskets
    return not in_basket


def _validate_basket(basket_dir, pantry):
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
    basket_dir: string
        the path in the file system to the basket root directory
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.

    Returns
    ----------
    boolean that is true when the Basket is valid
        if the Basket is invalid, raise a warning
    """
    manifest_path = os.path.join(basket_dir, 'basket_manifest.json')
    supplement_path = os.path.join(basket_dir, 'basket_supplement.json')

    # a valid basket has both manifest and supplement
    # if for some reason the manifest is gone, we get a wrong directory,
    # or this function is incorrectly called,
    # we can say that this isn't a basket.
    if not pantry.file_system.exists(manifest_path):
        raise FileNotFoundError(f"Invalid Path. "
                                f"No Basket found at: {basket_dir}")

    if not pantry.file_system.exists(supplement_path):
        warnings.warn(UserWarning(
            "Invalid Basket. No Supplement file found at: ", basket_dir))

    files_in_basket = pantry.file_system.ls(path=basket_dir, refresh=True)

    for file in files_in_basket:
        _, file_name = os.path.split(file)
        {
            "basket_manifest.json": _handle_manifest,
            "basket_supplement.json": _handle_supplement,
            "basket_metadata.json": _handle_metadata
        }.get(
            file_name, _handle_none_of_the_above
        )(file, pantry)

    # default return true if we don't find any problems with this basket
    return True


def _handle_manifest(file, pantry):
    """Handles case if manifest

    Parameters:
    -----------
    file: str
        Path to the file.
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
    """
    try:
        # Make sure it can be loaded, valid schema, and valid parent_uuids
        data = json.load(pantry.file_system.open(file))
        validate(instance=data, schema=manifest_schema)
        _validate_parent_uuids(data, pantry)

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


def _handle_supplement(file, pantry):
    """Handles case if supplement

    Parameters:
    -----------
    file: str
        Path to the file.
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
    """
    try:
        # these two lines make sure it can be read and is valid schema
        data = json.load(pantry.file_system.open(file))
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


def _handle_metadata(file, pantry):
    """Handles case if metadata

    Parameters:
    -----------
    file: str
        Path to the file.
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
    """
    try:
        json.load(pantry.file_system.open(file))

    except json.decoder.JSONDecodeError:
        warnings.warn(UserWarning(
            "Invalid Basket. "
            "Metadata could not be loaded into json at: ", file
        ))


def _handle_none_of_the_above(file, pantry):
    """Handles case if none of the above

    Parameters:
    -----------
    file: str
        Path to the file.
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
    """
    basket_dir, _ = os.path.split(file)
    if pantry.file_system.info(file)['type'] == 'directory':
        if _check_level(file,
                        pantry=pantry,
                        in_basket=True):
            warnings.warn(UserWarning(
                "Invalid Basket. Manifest File "
                "found in sub directory of basket at: ", basket_dir
            ))


def _validate_parent_uuids(data, pantry):
    """Validate that all the parent_uuids from the manifest exist in the pantry

    If there are parent uuids that don't actually exist in the pantry, we will
    raise a warning for each of those, along with the basket's uuid where we
    found the error.

    Parameters
    ----------
    data: dictionary
        the dictionary that contains the data of the manifest.json
    pantry: weave.Pantry
        Pantry object representing the pantry to validate.
    """
    # If there are no parent uuids in the manifest, no need to check anything
    if len(data["parent_uuids"]) == 0:
        return

    found_parents = list(pantry.index.get_row(data['parent_uuids'])['uuid'])
    missing_uids = [uid for uid in data["parent_uuids"] if uid not in found_parents]

    if missing_uids:
        warnings.warn(f"The uuids: {missing_uids} were not found in the "
                      f"index, which was found inside basket: {data['uuid']}")
