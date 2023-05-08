import uuid
import os

from weave.uploader import upload_basket

def upload(upload_items,
           basket_type,
           bucket_name = 'basket-data',
           parent_ids = [],
           metadata = {},
           label = ''):
    """
    Upload a basket of data to specified bucket in minio.
    
    Parameters
    ----------
    upload_items : [dict]
        List of python dictionaries with the following schema:
        {
            'path': path to the file or folder being uploaded (string),
            'stub': true/false (bool)
        }
        'path' can be a file or folder to be uploaded. Every filename
        and folder name must be unique. If 'stub' is set to True, integrity data
        will be included without uploading the actual file or folder. Stubs are 
        useful when original file source information is desired without uploading
        the data itself. This is especially useful when dealing with large files.
    bucket_name : str
        Name of the bucket that the basket will be uploaded to.
    basket_type: str
        Type of basket being uploaded.
    parent_ids: optional [str]
        List of unique ids associated with the parent baskets
        used to derive the new basket being uploaded.
    metadata: optional dict,
        Python dictionary that will be written to metadata.json
        and stored in the basket in MinIO.
    label: optional str,
        Optional user friendly label associated with the basket.
        
    Returns
    -------
    upload_directory : str
        Minio path to the basket after it has been uploaded. 
        This path will be of the form:
            bucket_name/basket_type/unique_id
    """
    #check data types for bucket_name. Other parameters are checked in upload_basket    
    if not isinstance(bucket_name, str):
        raise TypeError(f"'bucket_name' must be a string: '{bucket_name}'")
    
    #generate unique id
    unique_id = uuid.uuid1().hex
    
    #build upload directory of the form
    # bucket_name/basket_type/unique_id
    upload_directory = os.path.join(bucket_name, basket_type, unique_id)
    
    upload_basket(upload_items,
                 upload_directory,
                 unique_id,
                 basket_type,
                 parent_ids,
                 metadata,
                 label)
    
    return upload_directory
           