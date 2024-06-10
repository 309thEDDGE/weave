"""A Pantry Factory to build a Pantry object from default parameters or from a
pre-existing config file either locally or from the pantry path."""
import json
import os

import s3fs
from fsspec.implementations.local import LocalFileSystem

from .config import get_file_system
from .pantry import Pantry
from .index.index_pandas import IndexPandas
from .index.index_sqlite import IndexSQLite
from .index.index_sql import IndexSQL


def create_pantry(**kwargs):
    """Create a weave.Pantry object using regular params or a config file.

    Three options to create a pantry:
    1: Default args, requires Index, pantry_path, file_system
    2: A local config file which has information about the index, path, etc
    3: Only a pantry path and file_system, this will look for a global
       config file in the pantry root.

    Parameters:
    -----------
    **index: IndexABC (optional)
        The concrete implementation of an IndexABC. This is used to track
        the contents within the pantry.
    **pantry_path: str (optional)
        Path to the directory in which the pantry is located.
    **file_system: fsspec object (optional)
        The fsspec object which hosts the pantry we desire to index.
        If file_system is None, then the default fs is retrieved from the
        config.
    **config_file: str (optional)

    Returns:
    -----------
    weave.Pantry object
    """
    pantry = None

    # Create a pantry in the default way using the Pantry constructor
    if all(k in kwargs for k in ["index", "pantry_path", "file_system"]):
        pantry = Pantry(
            kwargs["index"],
            pantry_path=kwargs["pantry_path"],
            file_system=kwargs["file_system"],
        )
    # Create a pantry using a config file that is given to the pantry factory.
    elif "config_file" in kwargs:
        with open(kwargs["config_file"], "r", encoding="utf-8") as config_file:
            config = json.load(config_file)

        pantry = _create_pantry_from_config(config)
    # Create a pantry using a config that exists inside the pantry directory.
    elif "pantry_path" in kwargs:
        file_system = kwargs.get("file_system", None)
        if file_system is None:
            file_system = get_file_system()

        with file_system.open(
            os.path.join(kwargs["pantry_path"], "config.json"),
            'r'
        ) as config_file:
            config = json.load(config_file)

        pantry = _create_pantry_from_config(config)
    # If pantry was unable to be created using the methods above, throw error.
    else:
        raise ValueError("Invalid kwargs passed, unable to make pantry")
    return pantry


def _create_pantry_from_config(config):
    """Create the weave.pantry object from a pre-existing config file.
    """
    # Parse the index type used by this pantry.
    index_type = config["index"]
    if index_type == "IndexPandas":
        index_constructor = IndexPandas
    elif index_type == "IndexSQLite":
        index_constructor = IndexSQLite
    elif index_type == "IndexSQL":
        index_constructor = IndexSQL
    else:
        raise ValueError(f"Index Type '{index_type}' is not supported")

    # Get the pantry path.
    pantry_path = config["pantry_path"]

    # Get the file system used for this pantry.
    file_system_type = config["file_system"]
    if file_system_type == "LocalFileSystem":
        file_system = LocalFileSystem()
    elif file_system_type == "S3FileSystem":
        file_system = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": config["S3_ENDPOINT"]}
        )
    else:
        raise ValueError(f"File System Type: '{file_system_type}' is"
        f"not supported by this factory")

    # Get any additional index_kwargs to be passed forward.
    index_kwargs = config.get("index_kwargs", None)

    return Pantry(
        index_constructor,
        pantry_path=pantry_path,
        file_system=file_system,
        index_kwargs=index_kwargs,
    )
