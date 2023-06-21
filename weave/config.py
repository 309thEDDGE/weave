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


def get_file_system():
    """Get the filesystem to be used for storing baskets"""
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )

def get_mongo_db():
    """Get the mongodb client to be used for metadata search"""
    
    # If MONGODB_HOST, USERNAME and PASSWORD are provided as environment
    # variables, initialize the mongo client with the provided
    # credentials. Else defer to default credentials for OPAL.
    # TODO: remove the default credentials for OPAL, 
    # once OPAL exposes environment variables
    if "MONGODB_HOST" in os.environ and \
       "MONGODB_USERNAME" in os.environ and \
       "MONGODB_PASSWORD" in os.environ:
        client = pymongo.MongoClient(host = os.environ["MONGODB_HOST"],
                                     username = os.environ["MONGODB_USERNAME"],
                                     password = os.environ["MONGODB_PASSWORD"])
    else:
        client = pymongo.MongoClient("mongodb",
                                     username="root", 
                                     password="example")
    return client
