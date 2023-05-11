import s3fs
import os
file_system = s3fs.S3FileSystem(client_kwargs=
                               {"endpoint_url": os.environ["S3_ENDPOINT"]})
    
from .uploader import upload_basket
from .access import upload
from .create_index import create_index_from_s3
from .config import index_schema, get_file_system
