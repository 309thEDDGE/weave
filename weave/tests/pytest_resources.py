import os

import s3fs

from weave.uploader import upload_basket

# The following code is for testing in an environment with MinIO:


class BucketForTest():
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.basket_list = []
        ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
        self.s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
        self._set_up_bucket()

    def _set_up_bucket(self):
        try:
            self.s3_bucket_name = 'pytest-temp-bucket'
            self.s3fs_client.mkdir(self.s3_bucket_name)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()

    def set_up_basket(self, tmp_dir_name):
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        tmp_basket_txt_file = tmp_basket_dir.join("test.txt")
        tmp_basket_txt_file.write("This is a text file for testing purposes.")
        return tmp_basket_dir

    def add_lower_dir_to_temp_basket(self, tmp_basket_dir):
        nd = tmp_basket_dir.mkdir("nested_dir")
        nd.join("another_test.txt").write("more test text")
        return tmp_basket_dir

    def upload_basket(self, tmp_basket_dir, uid='0000', parent_ids=[]):
        b_type = "test_basket"
        up_dir = os.path.join(self.s3_bucket_name, b_type, uid)
        upload_basket(
            upload_items=[{'path':str(tmp_basket_dir.realpath()),
                           'stub':False}],
            upload_directory=up_dir,
            unique_id=uid,
            basket_type=b_type,
            parent_ids=parent_ids
        )
        return up_dir

    def cleanup_bucket(self):
        self.s3fs_client.rm(self.s3_bucket_name, recursive=True)


