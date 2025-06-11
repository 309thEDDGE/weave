"""Provides configuration settings used by Weave."""

import os

from fsspec.implementations.local import LocalFileSystem
import s3fs
# Try-Except required to make pymongo an optional dependency.
try:
    import pymongo
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True


# Filenames not allowed to be added to the basket.
# These files are taken for specific weave purposes.
prohibited_filenames = [
    "basket_manifest.json",
    "basket_metadata.json",
    "basket_supplement.json",
]

# basket_manifest must follow this schema
manifest_schema = {
    "properties": {
        "uuid": {"type": "string"},
        "upload_time": {"type": "string"},
        "parent_uuids": {"type": "array", "items": {"type": "string"}},
        "basket_type": {"type": "string"},
        "label": {"type": "string"},
        "weave_version": {"type": "string"}
    },
    "required": [
        "uuid",
        "upload_time",
        "parent_uuids",
        "basket_type",
        "label",
    ],
    "additionalProperties": False,
}

# basket_supplement must follow this schema
supplement_schema = {
    "properties": {
        "upload_items": {
            "type": "array",
            "minItems": 0,
            "items": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "stub": {"type": "boolean"},
                },
                "required": ["path", "stub"],
                "additionalProperties": False,
            },
        },
        "integrity_data": {
            "type": "array",
            "minItems": 0,
            "items": {
                "type": "object",
                "properties": {
                    "file_size": {"type": "number"},
                    "hash": {"type": "string"},
                    "access_date": {"type": "string"},
                    "source_path": {"type": "string"},
                    "byte_count": {"type": "number"},
                    "stub": {"type": "boolean"},
                    "upload_path": {"type": "string"},
                },
                "required": [
                    "file_size",
                    "hash",
                    "access_date",
                    "source_path",
                    "byte_count",
                    "stub",
                    "upload_path",
                ],
                "additionalProperties": False,
            },
            "required": ["type"],
            "additionalProperties": False,
        },
    },
    "required": ["upload_items", "integrity_data"],
    "additionalProperties": False,
}


def get_index_column_names():
    """Return index column names."""
    return ["uuid", "upload_time", "parent_uuids", "basket_type", "label",
            "weave_version", "address", "storage_type"]


def get_file_system(**kwargs):
    """Get the filesystem to be used for storing baskets.

    **file_system: str (default=s3)
        Selection of file system type. Must be s3 or local. Can also be set
        with the WEAVE_FILESYSTEM environment variable
    """
    file_system = kwargs.get("file_system", None)
    if file_system is None:
        file_system = os.environ.get("WEAVE_FILESYSTEM", "s3")
    if file_system == "s3":
        return s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
        )
    return LocalFileSystem()


def get_mongo_db(**kwargs):
    """Get the mongodb client to be used for metadata search.

    Parameters:
    -----------
    **timeout: int (default=0)
        Milliseconds to timeout the connection. Default of 0 means no timeout.
        Can also be set with the MONGO_TIMEOUT environment variable.
    """

    if not _HAS_PYMONGO:
        raise ImportError("Missing Dependency. The package 'pymongo' "
                          "is required to use this function")

    timeout = kwargs.get("timeout", None)
    if timeout is None:
        timeout = int(os.environ.get("WEAVE_MONGODB_TIMEOUT", 0))

    # If MONGODB_HOST, USERNAME and PASSWORD are provided as environment
    # variables, initialize the mongo client with the provided
    # credentials. Else defer to default credentials for OPAL.
    if (
        "MONGODB_HOST" in os.environ
        and "MONGODB_USERNAME" in os.environ
        and "MONGODB_PASSWORD" in os.environ
    ):
        client = pymongo.MongoClient(
            host=os.environ["MONGODB_HOST"],
            username=os.environ["MONGODB_USERNAME"],
            password=os.environ["MONGODB_PASSWORD"],
            port=int(os.environ.get("MONGODB_PORT", 27017)),
            timeoutMS=timeout,
        )
    else:
        raise KeyError("One or more of the environment variables "
                       "'MONGODB_HOST', 'MONGODB_USERNAME', and "
                       "'MONGODB_PASSWORD' are not set. "
                       "These are required to log into MongoDB")

    # Force a connection test before we return. This will raise an error if
    # the server is unreachable or has invalid credentials.
    client.server_info()
    return client
