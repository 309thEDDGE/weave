import pytest
import pandas as pd
import weave
from weave.tests.pytest_resources import BucketForTest

class MongoForTest(BucketForTest):
    """Extend the BucketForTest class to support mongodb and custom data
    loader"""
    def __init__(self, tmpdir):
        super().__init__(tmpdir)
        self.test_collection = 'test_collection'
        self.mongodb = weave.config.get_mongo_db().mongo_metadata
        self.load_data()

    def load_data(self):
        # Create a temporary basket with a test file.
        tmp_basket_dir_name = "test_basket_tmp_dir"
        tmp_basket_dir = self.set_up_basket(tmp_basket_dir_name)

        # Upload the basket with different uuids and metadata.
        self.upload_basket(tmp_basket_dir,
                           uid="1234",
                           metadata={'key1': 'value1'})

        tmp_nested_dir = self.add_lower_dir_to_temp_basket(tmp_basket_dir)
        self.upload_basket(tmp_nested_dir,
                           uid="4321",
                           metadata={'key2': 'value2'})

        self.upload_basket(tmp_basket_dir,
                           uid="nometadata")

    def cleanup(self):
        self.cleanup_bucket()
        self.mongodb[self.test_collection].drop()


@pytest.fixture
def set_up(tmpdir):
    db = MongoForTest(tmpdir)
    yield db
    db.cleanup()

def test_load_mongo(set_up):
    """
    Test that load_mongo successfully loads valid metadata to the db.
    """
    db = set_up
    index_table = weave.index.create_index_from_s3(db.s3_bucket_name)
    weave.load_mongo(index_table, db.test_collection)

    truth_db = [{'uuid': '1234',
                 'basket_type': 'test_basket',
                 'key1': 'value1'},
                {'uuid': '4321',
                 'basket_type': 'test_basket',
                 'key2': 'value2'}]

    db_data = list(db.mongodb[db.test_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop('_id')
        compared_data.append(item)

    assert truth_db == compared_data

def test_load_mongo_check_for_dataframe(set_up):
    """
    Test that load_mongo prevents loading data with an invalid index_table.
    """
    db = set_up
    with pytest.raises(
        TypeError, match="Invalid datatype for index_table: "
                         "must be Pandas DataFrame"
    ):
        weave.load_mongo("", db.test_collection)

def test_load_mongo_check_collection_for_string(set_up):
    """
    Test that load_mongo prevents loading data with an invalid db collection.
    """
    with pytest.raises(
        TypeError, match="Invalid datatype for collection: "
                         "must be a string"
    ):
        weave.load_mongo(pd.DataFrame(), 1)

def test_load_mongo_check_dataframe_for_uuid(set_up):
    """
    Test that load_mongo prevents loading data with missing uuid.
    """
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing uuid column"
    ):
        weave.load_mongo(pd.DataFrame({'basket_type': ['type'],
                                       'address': ['path']}),
                         db.test_collection)

def test_load_mongo_check_dataframe_for_address(set_up):
    """
    Test that load_mongo prevents loading data with missing address.
    """
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing address column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'],
                                       'basket_type': ['type']}),
                         db.test_collection)

def test_load_mongo_check_dataframe_for_basket_type(set_up):
    """
    Test that load_mongo prevents loading data with missing basket type.
    """
    db = set_up
    with pytest.raises(
        ValueError, match="Invalid index_table: "
                          "missing basket_type column"
    ):
        weave.load_mongo(pd.DataFrame({'uuid': ['1234'],
                                       'address': ['path']}),
                         db.test_collection)

def test_load_mongo_check_for_duplicate_uuid(set_up):
    """
    Test duplicate metadata won't be uploaded to mongoDB, based on the UUID.
    """
    db = set_up
    test_uuid = '1234'

    # Load metadata twice, and ensure there's only one instance
    index_table = weave.index.create_index_from_s3(db.s3_bucket_name)
    weave.load_mongo(index_table, db.test_collection)
    weave.load_mongo(index_table, db.test_collection)
    count = db.mongodb[db.test_collection].count_documents(
        {'uuid': test_uuid})
    assert count == 1, "duplicate uuid inserted"