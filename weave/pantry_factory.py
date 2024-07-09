"""A Pantry Factory to build a Pantry object from default parameters or from a
pre-existing config file either locally or from the pantry path."""
import json
import os
import warnings

import s3fs
from fsspec.implementations.local import LocalFileSystem
# Duplicate code below used to handle pymongo being unavailable.
# pylint: disable-next=duplicate-code
try:
    import pymongo
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

from .config import get_file_system
from .pantry import Pantry
from .index.index_pandas import IndexPandas
from .index.index_sqlite import IndexSQLite
from .index.index_sql import IndexSQL


def create_pantry(**kwargs):
    """Create a weave.Pantry object using regular params or a config file.

    Three options to create a pantry:
    1: Default args, requires an Index, and pantry_path (with an optional
       file_system kwarg).
    2: A local config file which has information about the index, path, etc
    3: Only a pantry path (with an optional file_system kwarg), this will look
       for a global config file in the pantry root.

    Parameters:
    -----------
    **index: IndexABC (optional)
        The concrete implementation of an IndexABC. This is used to track
        the contents within the pantry.
    **pantry_path: str (optional)
        Path to the directory in which the pantry is located.
    **file_system: fsspec object (optional)
        The fsspec object which hosts the pantry we desire to index.
        If the file system is not passed, then the default fs is retrieved from
        the weave config.
    **config_file: str (optional)
    Optional kwargs passed to the Index constructor.

    Returns:
    -----------
    weave.Pantry object
    """
    pantry = None

    # Create a pantry in the default way using the Pantry constructor
    if all(k in kwargs for k in ["index", "pantry_path"]):
        if "file_system" in kwargs:
            file_system = kwargs.pop("file_system")
        else:
            file_system = get_file_system()
        pantry = Pantry(
            kwargs.pop("index"),
            pantry_path=kwargs.pop("pantry_path"),
            file_system=file_system,
            **kwargs,
        )
    # Create a pantry using a config file that is given to the pantry factory.
    elif "config_file" in kwargs:
        with open(
            kwargs.pop("config_file"),
            "r",
            encoding="utf-8"
        ) as config_file:
            config = json.load(config_file)

        pantry = _create_pantry_from_config(config, **kwargs)
    # Create a pantry using a config that exists inside the pantry directory.
    elif "pantry_path" in kwargs:
        if "file_system" in kwargs:
            file_system = kwargs.pop("file_system")
        else:
            file_system = get_file_system()

        try:
            with file_system.open(
                os.path.join(kwargs.pop("pantry_path"), "config.json"),
                'r'
            ) as config_file:
                config = json.load(config_file)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                "Pantry path or config file does not exist. Please add a "
                "config file path or add a config file to the pantry."
            ) from exc
        pantry = _create_pantry_from_config(config, **kwargs)
    # If pantry was unable to be created using the methods above, throw error.
    else:
        raise ValueError("Invalid kwargs passed, unable to make pantry")
    return pantry

# pylint: disable-next=too-many-branches
def _create_pantry_from_config(config, **kwargs):
    """Create the weave.pantry object from a pre-existing config file.

    Parameters:
    -----------
    config: dict
        Dictionary that contains the contents of the config.json file.
    Optional kwargs passed to the Index constructor.

    Returns:
    -----------
    weave.Pantry object
    """
    # Ensure config has the proper keys and values:
    try:
        index_type = config["index"]
        pantry_path = config["pantry_path"]
        file_system_type = config["file_system"]
    except KeyError as exc:
        raise KeyError(
            "Config file requires 'index' 'pantry_path' and "
            "'file_system' keys to build pantry from config file."
        ) from exc

    # Remove pantry kwargs to not conflict with config args.
    _ = [kwargs.pop(x, None) for x in ["index", "pantry_path", "file_system"]]

    # Parse the index type used by this pantry.
    if index_type == "IndexPandas":
        index_constructor = IndexPandas
    elif index_type == "IndexSQLite":
        index_constructor = IndexSQLite
    elif index_type == "IndexSQL":
        index_constructor = IndexSQL
    else:
        raise ValueError(f"Index Type '{index_type}' is not supported")

    # Get the file system used for this pantry.
    if file_system_type == "LocalFileSystem":
        file_system = LocalFileSystem()
    elif file_system_type == "S3FileSystem":
        if "S3_ENDPOINT" in config:
            file_system = s3fs.S3FileSystem(
                client_kwargs={"endpoint_url": config["S3_ENDPOINT"]}
            )
        else:
            # If there is no custom endpoint, use the default one in the config
            file_system = get_file_system()
    else:
        raise ValueError(f"File System Type: '{file_system_type}' is"
        f"not supported by this factory")

    # Check for optional mongo db connection config keys.
    mongo_client = None
    if "mongodb_host" in config:
        if not _HAS_PYMONGO:
            warnings.warn(
                UserWarning(
                    "Found mongo configuration keys, but pymongo is not "
                    "available on this system... Ignoring mongo config."
                )
        )
        else:
            mongo_client = pymongo.MongoClient(
                host=config["mongodb_host"],
                username=config["mongodb_username"],
                password=config["mongodb_password"],
                port=config.get("mongodb_port", 27017),
            )

    return Pantry(
        index_constructor,
        pantry_path=pantry_path,
        file_system=file_system,
        mongo_client=mongo_client,
        **kwargs,
    )
