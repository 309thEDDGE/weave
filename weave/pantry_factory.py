import json

from .pantry import Pantry
from .index.index_pandas import IndexPandas
from .index.index_sqlite import IndexSQLite
from .index.index_sql import IndexSQL


class PantryFactory():
    def __init__(self, **kwargs):

        # Three options to create a pantry:
        # 1: Default args, requires Index, pantry_path, file_system
        # 2: A local config file which has information about the index, path, etc
        # 3: Only a pantry path and file_system, this will look for a global config file in the pantry root.

        if all(k in kwargs for k in ["index", "pantry_path", "file_system"]):
            return Pantry(
                kwargs["index"],
                pantry_path=kwargs["pantry_path"],
                file_system=kwargs["file_system"],
            )

        elif "config_file" in kwargs:
            with open(kwargs["config_file"], "r") as config_file:
                config = json.load(config_file)

            return self._create_pantry_from_config(config)

        elif "pantry_path" in kwargs:
            if "file_system" in kwargs:
                file_system = kwargs["file_system"]
            else:
                file_system = config.get_file_system()

            with file_system.open(
                os.path.join(kwargs["pantry_path"], "config.json"),
                'r'
            ) as config_file:
                config = json.load(config_file)

            return self._create_pantry_from_config(config)

    def _create_pantry_from_config(self, config):
        # Parse the index type used by this pantry.
        index_type = config["index"]
        if index_type == "IndexPandas":
            index_constructor = IndexPandas
        elif index_type == "IndexSQLite":
            index_constructor = IndexSQLite
        elif index_type == "IndexSQL":
            index_constructor = IndexSQL
        else
            raise ValueError(f"Index Type '{index_type}' is not supported")

        # Get the pantry path.
        pantry_path = config["pantry_path"]

        # Get the file system used for this pantry.
        file_system_type = kwargs["file_system"]
        if file_system_type == "LocalFileSystem":
            file_system = LocalFileSystem()
        elif file_system_type == "S3FileSystem":
            file_system = s3fs.S3FileSystem(
                client_kwargs={"endpoint_url": config["S3_ENDPOINT"]}
            )
        else:
            raise ValueError(f"File System Type: '{file_system_type}' is
            not supported by this factory")

        # Get any additional index_kwargs to be passed forward.
        index_kwargs = config.get("index_kwargs", None)

        return Pantry(
            index_constructor,
            pantry_path=pantry_path,
            file_system=file_system,
            index_kwargs=index_kwargs,
        )
