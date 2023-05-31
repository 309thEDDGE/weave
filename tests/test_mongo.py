import pytest
import pymongo
import tempfile
import os
from weave.mongo import load_mongo
from weave.create_index import create_index_from_s3
from weave.uploader import upload_basket
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch


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
        #self.client = pymongo.MongoClient('mongodb://localhost:27017/')  
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
    def test_load_mongo(self, patch):
        index_table = create_index_from_s3(self.bucket_path)
        load_mongo(index_table)
    
    
    
#     #setup db client
#     @pytest.fixture(scope="class")
#     def client_setup(self):
#         client = pymongo.MongoClient("mongodb", username="root", password="example")
#         yield client
#         #remove test_database
#         client.drop_database('test_database')
        
#     #insert data into the db and store the id    
#     @pytest.fixture
#     def dataId(self, client_setup, data):
       
#         db = client_setup.test_database
#         return db.developers.insert_one(data).inserted_id
    
#     #create the test_database database
#     @pytest.fixture
#     def db_setup(self,client_setup):
#         db = client_setup.test_database
#         return db
    
#     #create the test data
#     @pytest.fixture
#     def data(self):
#         data = {
#              "developer":"FirstName.LastName",
#              "email":"FirstName.LastName@fakeEmail",
#              "jira_ticket":1017,
#              "tags": ["dev", "ops", "analysis"],
#              }
#         return data
        
#     #create the developers collection    
#     @pytest.fixture
#     def dev_setup(self,db_setup):
#         dev = db_setup.developers
#         return dev

#     #test when getting the one piece of data in the developers collection under the test_database that it matches the id of the one that was inserted
#     def test_findOne(self,dev_setup,client_setup,dataId):
#         foundData = dev_setup.find_one()
#         assert foundData['_id'] == dataId
    
#     #verify the developer data
#     def test_findDeveloper(self,dev_setup,client_setup,data):
#         foundData = dev_setup.find_one()
#         assert foundData['developer'] == data['developer']
        
#     #verify the email data    
#     def test_findEmail(self,dev_setup,client_setup,data):
#         foundData = dev_setup.find_one()
#         assert foundData['email'] == data['email']
        
#     #verify the jira ticket data    
#     def test_findJira_ticket(self,dev_setup,client_setup,data):
#         foundData = dev_setup.find_one()
#         assert foundData['jira_ticket'] == data['jira_ticket']
    
#     #verify the tag data
#     def test_findTags(self,dev_setup,client_setup,data):
#         foundData = dev_setup.find_one()
#         assert foundData['tags'] == data['tags']