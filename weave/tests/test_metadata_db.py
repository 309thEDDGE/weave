import pytest
import pandas as pd
import weave
from weave.config import get_file_system

class MongoForTest():
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.s3fs_client = get_file_system()
        
        self.test_collection = 'test_collection'
        self.mongodb = weave.config.get_mongo_db().mongo_metadata
        
        self.basket_type = "test_basket_type"
        self.test_bucket = "test-bucket"
        self._set_up_bucket()

        self.local_dir_path = self.tmpdir.mkdir("test_dir_local")
        self.tmp_basket_txt_file = self.local_dir_path.join("test.txt")
        self.tmp_basket_txt_file.write("This is a file for testing purposes.")
        self.load_data()
    
    def load_data(self):
        weave.uploader.upload_basket(
            [{"path": self.tmp_basket_txt_file.strpath, "stub": False}],
            f"{self.test_bucket}/{self.basket_type}/1234",
            "1234",
            self.basket_type,
            metadata={'key1': 'value1'}
        )

        weave.uploader.upload_basket(
            [{"path": self.tmp_basket_txt_file.strpath, "stub": False}],
            f"{self.test_bucket}/{self.basket_type}/one_deeper/4321",
            "4321",
            self.basket_type,
            metadata={'key2': 'value2'}
        )
        
        # No Metadata
        weave.uploader.upload_basket(
            [{"path": self.tmp_basket_txt_file.strpath, "stub": False}],
            f"{self.test_bucket}/{self.basket_type}/nometadata",
            "nometadata",
            self.basket_type,
        )
    
    def _set_up_bucket(self):
        try:
            self.s3fs_client.mkdir(self.test_bucket)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()
    
    def cleanup_bucket(self):
        self.s3fs_client.rm(self.test_bucket, recursive=True)
        self.mongodb[self.test_collection].drop()


@pytest.fixture
def set_up(tmpdir):
    db = MongoForTest(tmpdir)
    yield db
    db.cleanup_bucket()

def test_load_mongo(set_up):
    db = set_up
    index_table = weave.index.create_index_from_s3(db.test_bucket)
    weave.load_mongo(index_table, db.test_collection)
    
    truth_db = [{'uuid': '1234', 'basket_type': 'test_basket_type', 
                 'key1': 'value1'}, 
                {'uuid': '4321', 'basket_type': 'test_basket_type', 
                 'key2': 'value2'}]

    db_data = list(db.mongodb[db.test_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop('_id')
        compared_data.append(item)

    assert truth_db == compared_data        

def test_load_mongo_check_for_dataframe(set_up):
    db = set_up
    with pytest.raises(
        TypeError, match="Invalid datatype for index_table: "
                         "must be Pandas DataFrame"
    ):
        weave.load_mongo("", db.test_collection)

def test_load_mongo_check_collection_for_string(set_up):
    set_up
    with pytest.raises(
        TypeError, match="Invalid datatype for collection: "
                         "must be a string"
    ):
        weave.load_mongo(pd.DataFrame(), 1)

def test_load_mongo_check_dataframe_for_uuid(set_up):
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing uuid column"
    ):
        weave.load_mongo(pd.DataFrame({'basket_type': ['type'], 
                                       'address': ['path']}),
                         db.test_collection)

def test_load_mongo_check_dataframe_for_address(set_up):
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing address column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'], 
                                       'basket_type': ['type']}),
                         db.test_collection)

def test_load_mongo_check_dataframe_for_basket_type(set_up):
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing basket_type column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'], 
                                       'address': ['path']}),
                         db.test_collection)
        
def test_load_mongo_check_for_duplicate_uuid(set_up):
    db = set_up
    test_uuid = '1234'

    # Load metadata twice, and ensure there's only one instance
    index_table = weave.index.create_index_from_s3(db.test_bucket)
    weave.load_mongo(index_table, db.test_collection)
    weave.load_mongo(index_table, db.test_collection)
    count = db.mongodb[db.test_collection].count_documents(
        {'uuid': test_uuid})
    assert count == 1, "duplicate uuid inserted"