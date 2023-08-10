"""Contains upload function, which accesses uploader_functions to tie together
all uploading functionality (outside of that exposed by index.py)
"""

import uuid
import os

from .uploader_functions import UploadBasket

def upload(
    upload_items,
    basket_type,
    bucket_name="basket-data",
    **kwargs,
):
    """
    Upload a basket of data to specified bucket in the upload fs.

    Parameters
    ----------
    upload_items : [dict]
        List of python dictionaries with the following schema:
        {
            'path': path to the file or folder being uploaded (string),
            'stub': true/false (bool)
        }
        'path' can be a file or folder to be uploaded. Every filename
        and folder name must be unique. If 'stub' is set to True, integrity
        data will be included without uploading the actual file or folder.
        Stubs are useful when original file source information is desired
        without uploading the data itself. This is especially useful when
        dealing with large files.
    basket_type: str
        Type of basket being uploaded.
    bucket_name : str
        Name of the bucket that the basket will be uploaded to.
    parent_ids: optional [str]
        List of unique ids associated with the parent baskets
        used to derive the new basket being uploaded.
    metadata: optional dict,
        Python dictionary that will be written to metadata.json
        and stored in the basket in the upload fs.
    label: optional str,
        Optional user friendly label associated with the basket.

    kwargs:
    file_system: fsspec object
        The file system to upload to (ie s3fs, local fs, etc).
        If None it will use the default fs from the config.

    Returns
    -------
    upload_directory : str
        The path to the basket after it has been uploaded.
        This path will be of the form:
            bucket_name/basket_type/unique_id
    """
    # check data types for bucket_name.
    # Other parameters are checked in upload_basket
    if not isinstance(bucket_name, str):
        raise TypeError(f"'bucket_name' must be a string: '{bucket_name}'")

    # generate unique id
    unique_id = uuid.uuid1().hex

    prefix = kwargs.get("test_prefix", "")

    # build upload directory of the form
    # bucket_name/basket_type/unique_id
    upload_directory = os.path.join(
        prefix, bucket_name, basket_type, unique_id
    )

    UploadBasket(
        upload_items,
        upload_directory=upload_directory,
        unique_id=unique_id,
        basket_type=basket_type,
        **kwargs
    )

    return upload_directory
