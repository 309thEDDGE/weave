from .uploader_functions import *

def upload_basket(upload_items, upload_directory, unique_id, basket_type,
                  parent_ids = [], metadata = {}, label = '', **kwargs):
    """
    Upload files and directories to MinIO. 

    This function takes in a list of items to upload along with
    taging information and uploads the data together with three
    json files: basket_manifest.json, basket_metadata.json, and
    supplement.json. The contents of the three files are specified 
    below. These three files together with the data specified by
    upload_items are uploaded to the upload_directory path within 
    MinIO as a basket of data. 

    basket_manifest.json contains:
        1) unique_id
        2) list of parent ids
        3) basket type
        4) label
        5) upload date

    basket_metadata.json contains:
        1) dictionary passed in through the metadata parameter

    basket_supplement.json contains:
        1) the upload_items object that was passed as an input parameter
        2) integrity_data for every file uploaded 
            a) file checksum
            b) upload date
            c) file size (bytes)
            d) source path (original file location) 
            e) stub (true/false)
            f) upload path (path to where the file is uploaded in MinIO)

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
        useful when original file source information is desired without
        uploading the data itself. This is especially useful when dealing with
        large files.
    upload_directory: str
        MinIO path where basket is to be uploaded.
    unique_id: str
        Unique ID to identify the basket once uploaded.
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
    """
    upload_basket_obj = UploadBasket(upload_items, upload_directory,
                                     unique_id, basket_type, parent_ids,
                                     metadata, label, **kwargs)

    upload_basket_obj.sanitize_args()
    upload_basket_obj.establish_s3fs()
    upload_basket_obj.check_that_upload_dir_does_not_exist()

    try:
        upload_basket_obj.setup_temp_dir_for_staging_prior_to_s3fs()
        upload_basket_obj.upload_files_and_stubs_to_s3fs()
        upload_basket_obj.create_and_upload_basket_json_to_s3fs()
        upload_basket_obj.upload_basket_metadata_to_s3fs()
        upload_basket_obj.upload_basket_supplement_to_s3fs()

        if upload_basket_obj.test_clean_up:
            raise Exception('Test Clean Up')

    except Exception as e:
        if upload_basket_obj.s3fs_upload_path_exists():
            upload_basket_obj.clean_out_s3fs_upload_dir()
        raise e

    finally:
        upload_basket_obj.tear_down_temp_dir()
