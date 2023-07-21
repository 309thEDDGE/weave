import os
import json

from weave.uploader import upload_basket
from weave.config import get_file_system

# The following code is for testing in an environment with MinIO:

class BucketForTest():
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.basket_list = []
        self.fs = get_file_system()
        self.bucket_name = 'pytest-temp-bucket'
        self._set_up_bucket()

    def _set_up_bucket(self):
        """
        Create a temporary S3 Bucket for testing purposes.
        """
        try:
            self.fs.mkdir(self.bucket_name)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()

    def set_up_basket(self, tmp_dir_name,
                      file_name="test.txt", file_content="This is a test"):
        """
        Create a temporary (local) basket, with a single text file.
        """
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        tmp_basket_txt_file = tmp_basket_dir.join(file_name)

        if file_name[file_name.rfind('.'):] == ".json":
            with open(tmp_basket_txt_file, "w") as outfile:
                json.dump(file_content, outfile)
        else:
            tmp_basket_txt_file.write(file_content)

        return tmp_basket_dir

    def add_lower_dir_to_temp_basket(self, tmp_basket_dir):
        """
        Add a nested directory inside the temporary basket.
        """
        nd = tmp_basket_dir.mkdir("nested_dir")
        nd.join("another_test.txt").write("more test text")
        return tmp_basket_dir

    def upload_basket(self, tmp_basket_dir,
                      uid='0000', parent_ids=[],
                      upload_items=None, metadata={}):
        """
        Upload a temporary (local) basket to the S3 test bucket.
        """
        b_type = "test_basket"
        up_dir = os.path.join(self.bucket_name, b_type, uid)

        if upload_items is None:
            upload_items = [{'path':str(tmp_basket_dir.realpath()),
                           'stub':False}]

        upload_basket(
            upload_items=upload_items,
            upload_directory=up_dir,
            unique_id=uid,
            basket_type=b_type,
            parent_ids=parent_ids,
            metadata=metadata
        )
        return up_dir

    def cleanup_bucket(self):
        """
        Delete the temporary test bucket, including any uploaded baskets.
        """
        self.fs.rm(self.bucket_name, recursive=True)