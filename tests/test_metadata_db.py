import tempfile
import os
import mongomock
from weave.metadata_db import load_mongo
from weave.create_index import create_index_from_s3
from weave.uploader import upload_basket
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch

mock_db = mongomock.MongoClient().db

class TestMongo():
    
    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def load_data(self, patch):
        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/1234",
            "1234",
            self.basket_type,
            metadata={'key1': 'value1'}
        )

        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/one_deeper/4321",
            "4321",
            self.basket_type,
            metadata={'key2': 'value2'}
        )
    
    def setup_class(self):
        self.fs = LocalFileSystem()
        self.basket_type = "test_basket_type"
        self.test_bucket = "test-bucket"

        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name

        self.bucket_path = os.path.join(
            self.file_system_dir_path, self.test_bucket
        )

        # make sure bucket doesn't exist. if it does, delete it.
        if self.fs.exists(f"{self.bucket_path}"):
            self.fs.rm(f"{self.bucket_path}", recursive=True)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name
    
    def setup_method(self):
        self.load_data()        
    
    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    @patch("weave.config.get_mongo_db", return_value=mock_db)
    def test_load_mongo(self, patch1, patch2):
        index_table = create_index_from_s3(self.bucket_path)
        load_mongo(index_table)
        truth_db = [{'uuid': '1234', 'basket_type': 'test_basket_type', 
                     'key1': 'value1'}, 
                    {'uuid': '4321', 'basket_type': 'test_basket_type', 
                     'key2': 'value2'}]
        db_data = list(mock_db.metadata.find({}))
        compared_data = []
        for item in db_data:
            item.pop('_id')
            compared_data.append(item)
            
        assert truth_db == compared_data
