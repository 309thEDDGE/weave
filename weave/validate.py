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
    pantry_name: string
        the name of the pantry we are validating
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

    # Here we are catching the warnings that are shown from calling
    # generate_index() because we don't want to show the same warning twice
    ind = Index(pantry_name=pantry_name, file_system=file_system)
    with warnings.catch_warnings(record=True):
        try:
            ind.generate_index()
        except json.decoder.JSONDecodeError as error:
            raise ValueError(
                f"Pantry could not be loaded into index: {error}"
            ) from error
    index_df = ind.to_pandas_df()

    # Call check level, with a path, but since we're just starting,
    # We just use the pantry_name as the path
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
    current_dir: string
        the current directory that we want to search all files and
        directories of

    kwargs:
    file_system: fsspec object
        the file system (s3fs, local fs, etc.) that we want to search all files
        and directories of
    index_df: dataframe
        a dataframe representing the index
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
    file_system = kwargs.get("file_system")
    index_df = kwargs.get("index_df")
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
        return _validate_basket(current_dir, file_system, index_df)

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
                                    file_system=file_system,
                                    index_df=index_df,
                                    in_basket=in_basket)
            # if we aren't in the basket, we want to check all files in our
            # current dir. If everything is valid, _check_level returns true
            # if it isn't valid, we go in and return false
            # we don't want to return _check_level because we want to keep
            # looking at all the sub-directories
            if not _check_level(file_or_dir,
                                file_system=file_system,
                                index_df=index_df,
                                in_basket=in_basket):
                return False

    # This is the default backup return.
    # If we are in a basket, it will be valid if we return false,
    # because we want to signify that we didn't find another basket
    # If we are not in a basket, we want to return true, because
    # we didn't find a basket and it was valid to have no baskets
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
    basket_dir: string
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

    # a valid basket has both manifest and supplement
    # if for some reason the manifest is gone, we get a wrong directory,
    # or this function is incorrectly called,
    # we can say that this isn't a basket.
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

    # default return true if we don't find any problems with this basket
    return True


def _handle_manifest(file, file_system, index_df):
    """Handles case if manifest

    Parameters:
    -----------
    file: str
        Path to the file.
    file_system: fsspec-like obj
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


def _handle_supplement(pantry_name, file, file_system, _index_df):

    """Handles case if supplement

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
        # these two lines make sure it can be read and is valid schema
        data = json.load(file_system.open(file))
        validate(instance=data, schema=supplement_schema)
        basket_dir, _ = os.path.split(file)
        # _validate_supplement_files(pantry_name, basket_dir, data, file_system)

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
    file_system: fsspec-like obj
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

    If there are parent uuids that don't actually exist in the pantry, we will
    raise a warning for each of those, along with the basket's uuid where we
    found the error.

    Parameters
    ----------
    data: dictionary
        the dictionary that contains the data of the manifest.json
    file_system: fsspec-like obj
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


def _validate_supplement_files(pantry_name, basket_dir, data, file_system):
    """Validate the files listed in the supplement's integrity_data

    Parameters
    ----------
    basket_dir: str
        the path to the current working basket
    data: dictionary
        the dictionary that contains the data of the supplement.json
    file_system: fsspec-like obj
        The file system to use.
    """
    sys_file_list = file_system.find(path=basket_dir, withdirs=False)

    manifest_path = os.path.join(basket_dir, "basket_manifest.json")
    supplement_path = os.path.join(basket_dir, "basket_supplement.json")
    metadata_path = os.path.join(basket_dir, "basket_metadata.json")

    # Grab all the files, but remove manifest, supplement, and metadata
    system_file_list = [
        file for file in sys_file_list if file not in [manifest_path,
                                                       supplement_path,
                                                       metadata_path]
    ]

    supp_file_list = [file["upload_path"] for file in data["integrity_data"]]
    
    
    
    
    # print('\n\npantry name: ', pantry_name)
    # print('basket_dir : ', basket_dir)
    
    
    
    
    
#     system_file_list = []
#     supp_file_list = []
    
#     for i in range(10):
#         supp_file_list.append(f"pytest-temp-bucket/test_basket/0000/file_0{i}.txt")
#         if (str(type(file_system)) == "<class 's3fs.core.S3FileSystem'>"):
#             system_file_list.append(f"pytest-temp-bucket/test_basket/0000/file_0{i}.txt")
#         else:
#             system_file_list.append(f"home/joyvan/pytest-temp-bucket/test_basket/0000/file_0{i}.txt")

#     for i in range(2):
#         supp_file_list.append(f"pytest-temp-bucket/test_basket/0000/IN_SUPP_{i}.txt")
#         if (str(type(file_system)) == "<class 's3fs.core.S3FileSystem'>"):
#             system_file_list.append(f"pytest-temp-bucket/test_basket/0000/IN_SYS_{i}.txt")
#         else:
#             system_file_list.append(f"home/joyvan/pytest-temp-bucket/test_basket/0000/IN_SYS_{i}.txt")

#     print('\n\tTEST_SUPP_LIST:')
#     for i in supp_file_list:
#         print(i)

#     print('\n\tTEST_SYS_LIST:')
#     for j in system_file_list:
#         print(j)


    # new_system_file_list = []
    
    system_file_list = [file[file.find(pantry_name):] for file in system_file_list]
    supp_file_list = [file[file.find(pantry_name):] for file in supp_file_list]
    
    system_file_set = set(system_file_list)
    supp_file_set = set(supp_file_list)
    
    files_not_in_system = supp_file_set - system_file_set
    files_not_in_supp = system_file_set - supp_file_set
    
    for file in files_not_in_system:
        warnings.warn(
            UserWarning("File listed in the basket_supplement.json does not "
                        "exist in the file system: ", file)
        )
    
    for file in files_not_in_supp:
        warnings.warn(
            UserWarning("File found in the file system is not listed in "
                        "the basket_supplement.json: ", file)
        )
    
    # print("files not in system: ", files_not_in_system)
    # print("files not in supplement: ", files_not_in_supp)
    
    
#     for k in system_file_list:
        
#         # new_system_file_list.append(os.path.relpath(k, pantry_name))
#         new_system_file_list.append(k[k.find(pantry_name):])
        
    
    # print('\n\tNEW_SYS_LIST:')
    # for i in system_file_list:
    #     print(i)








#     print('\nsystem file list: length: ', len(system_file_list))
#     for i in system_file_list:
#         print(i)

#     print('\n\nsupp file list: len: ', len(supp_file_list))
#     for i in supp_file_list:
#         print(i)


"""
    # Check if all the system files exist in the supplement file list
    for sys_file in system_file_list:
        if list(filter(sys_file.endswith, supp_file_list)) == []:
            # print("File found in the file system is not listed in "
            #       "the basket_supplement.json: ", sys_file)
            warnings.warn(
                UserWarning("File found in the file system is not listed in "
                            "the basket_supplement.json: ", sys_file)
            )

    # Collect all the correct supplement files
    correct_supp_files = []
    for supp_file in supp_file_list:
        for sys_file in system_file_list:
            if sys_file.endswith(supp_file):
                correct_supp_files.append(supp_file)

    wrong_supp_files = [
        x for x in supp_file_list if x not in correct_supp_files
    ]

    for file in wrong_supp_files:
        # print("File listed in the basket_supplement.json does not "
        #       "exist in the file system: ", file)
        warnings.warn(
             UserWarning("File listed in the basket_supplement.json does not "
                         "exist in the file system: ", file)
         )
         """