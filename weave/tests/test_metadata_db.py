import os
import tempfile
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch
import pytest
import pandas as pd
import weave

class TestMongo():
    
    def load_data(self):
        weave.uploader.upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/1234",
            "1234",
            self.basket_type,
            metadata={'key1': 'value1'}
        )

        weave.uploader.upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/one_deeper/4321",
            "4321",
            self.basket_type,
            metadata={'key2': 'value2'}
        )
            
        # No Metadata
        weave.uploader.upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/nometadata",
            "nometadata",
            self.basket_type,
        )
    
    def setup_class(self):
        self.test_collection = 'test_collection'
        self.mongodb = weave.config.get_mongo_db().mongo_metadata
        
        self.fs = LocalFileSystem()
        self.basket_type = "test_basket_type"
        self.test_bucket = "test-bucket"

        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name

        self.bucket_path = os.path.join(
            self.file_system_dir_path, self.test_bucket
        )

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name
    
    def setup_method(self):
        # make sure bucket doesn't exist. if it does, delete it.
        if self.fs.exists(f"{self.bucket_path}"):
            self.fs.rm(f"{self.bucket_path}", recursive=True)
        self.load_data()        
    
    def teardown_method(self):
        if self.fs.exists(f"{self.bucket_path}"):
            self.fs.rm(f"{self.bucket_path}", recursive=True)
            
        self.mongodb[self.test_collection].drop()


@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()


def test_load_mongo(set_up_tb):
    index_table = weave.index.create_index_from_s3(self.bucket_path)
    weave.load_mongo(index_table, self.test_collection)
    truth_db = [{'uuid': '1234', 'basket_type': 'test_basket_type', 
                 'key1': 'value1'}, 
                {'uuid': '4321', 'basket_type': 'test_basket_type', 
                 'key2': 'value2'}]

    db_data = list(self.mongodb[self.test_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop('_id')
        compared_data.append(item)

    assert truth_db == compared_data        

def test_load_mongo_check_for_dataframe(set_up_tb):
    with pytest.raises(
        TypeError, match="Invalid datatype for index_table: "
                         "must be Pandas DataFrame"
    ):
        weave.load_mongo("", self.test_collection)

def test_load_mongo_check_collection_for_string(set_up_tb):
    with pytest.raises(
        TypeError, match="Invalid datatype for collection: "
                         "must be a string"
    ):
        weave.load_mongo(pd.DataFrame(), 1)

def test_load_mongo_check_dataframe_for_uuid(set_up_tb):
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing uuid column"
    ):
        weave.load_mongo(pd.DataFrame({'basket_type': ['type'], 
                                       'address': ['path']}),
                         self.test_collection)

def test_load_mongo_check_dataframe_for_address(set_up_tb):
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing address column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'], 
                                       'basket_type': ['type']}),
                         self.test_collection)

def test_load_mongo_check_dataframe_for_basket_type(set_up_tb):
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing basket_type column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'], 
                                       'address': ['path']}),
                         self.test_collection)

def test_load_mongo_check_for_duplicate_uuid(set_up_tb):
    test_uuid = '1234'

    # Create the test basket.
    # weave.uploader.upload_basket(
    #     [{"path": self.local_dir_path, "stub": False}],
    #     f"{self.bucket_path}/{self.basket_type}/{test_uuid}",
    #     test_uuid,
    #     self.basket_type,
    #     metadata={'key1': 'value1'}
    # )

    # test_metadata = pd.DataFrame(
    #     {'uuid': [test_uuid],
    #     'address': [f"{self.bucket_path}/{self.basket_type}/{test_uuid}"],
    #     'basket_type': ['type']})

    # Load metadata again, and ensure there's still only one instance
    index_table = weave.index.create_index_from_s3(self.bucket_path)
    weave.load_mongo(index_table, self.test_collection)
    weave.load_mongo(index_table, self.test_collection)
    count = self.mongodb[self.test_collection].count_documents(
        {'uuid': test_uuid})
    assert count == 1, "duplicate uuid inserted"
        
        