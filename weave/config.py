"""
config.py provides configuration settings used by weave.
"""
import s3fs
import os
import pymongo

# Filenames not allowed to be added to the basket.
# These files are taken for specific weave purposes.
prohibited_filenames = [
    "basket_manifest.json",
    "basket_metadata.json",
    "basket_supplement.json",
]


# As schema change, a parameter can be passed to index_schema
# to determine which schema to return.
def index_schema():
    """
    Return the keys expected from the manifest.json file.
    """
    return ["uuid", "upload_time", "parent_uuids", "basket_type", "label"]


# Get the filesystem to be used for storing baskets
def get_file_system():
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )

# Get the mongodb object
def get_mongo_db():
    client = pymongo.MongoClient("mongodb", 
                                 username="root", 
                                 password="example")
    return client.mongo_database