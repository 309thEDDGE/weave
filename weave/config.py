"""Provides configuration settings used by Weave."""

import os

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


def get_file_system():
    """Get the filesystem to be used for storing baskets."""
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )


def get_mongo_db():
    """Get the mongodb client to be used for metadata search."""

    if not _HAS_PYMONGO:
        raise ImportError("Missing Dependency. The package 'pymongo' "
                          "is required to use this function")

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
        )
    else:
        client = pymongo.MongoClient(
            "mongodb", username="root", password="example"
        )
    return client
