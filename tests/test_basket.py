import tempfile
import pytest
import os
import json
from unittest.mock import patch
from fsspec.implementations.local import LocalFileSystem
from weave.basket import Basket
from weave.access import upload

from weave.uploader import upload_basket

class TestBasket():
    def setup_class(self):
        self.fs = LocalFileSystem()       
        self.basket_type = 'test_basket_type'
        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name
        self.test_bucket = os.path.join(self.file_system_dir_path, 'pytest-bucket')
        self.fs.mkdir(self.test_bucket)
        self.uuid = '1234'
        self.basket_path = os.path.join(self.test_bucket, self.basket_type, self.uuid)
        
    def setup_method(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        if self.fs.exists(self.basket_path):
            self.fs.rm(self.basket_path, recursive = True)
            
    def teardown_method(self):
        if self.fs.exists(self.basket_path):
            self.fs.rm(self.basket_path, recursive = True)
        self.temp_dir.cleanup()        

    def teardown_class(self):
        if self.fs.exists(self.test_bucket):
            self.fs.rm(self.test_bucket, recursive = True)
        self.file_system_dir.cleanup()
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_address_does_not_exist(self, patch):
        basket_path = 'i n v a l i d p a t h'
        with pytest.raises(
            ValueError,
            match = f"Basket does not exist: {basket_path}"
        ):
            basket = Basket(basket_path)
            
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_validate_basket_no_manifest_file(self, patch):
        #upload basket
        upload_basket([{"path": self.temp_dir_path, "stub": False}],
                        self.basket_path, self.uuid, self.basket_type)
        
        manifest_path = os.path.join(self.basket_path, 'basket_manifest.json')
        os.remove(manifest_path)
        
        with pytest.raises(
            FileNotFoundError,
            match = f"Invalid Basket, basket_manifest.json does not exist: {manifest_path}"
        ):
            basket = Basket(self.basket_path)
            
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_validate_basket_no_suppl_file(self, patch):
        #upload basket
        upload_basket([{"path": self.temp_dir_path, "stub": False}],
                        self.basket_path, self.uuid, self.basket_type)
        
        supplement_path = os.path.join(self.basket_path, 'basket_supplement.json')
        os.remove(supplement_path)
        
        with pytest.raises(
            FileNotFoundError,
            match = f"Invalid Basket, basket_supplement.json does not exist: {supplement_path}"
        ):
            basket = Basket(self.basket_path)
            
    # Test init function takes in a string
    # Test caching reads once for each file
    # Test LS
    
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_manifest(self, patch):
        #upload basket
        upload_basket([{"path": self.temp_dir_path, "stub": False}],
                        self.basket_path, self.uuid, self.basket_type)
        
        basket = Basket(self.basket_path)
        manifest = basket.get_manifest()
        assert 'upload_time' in manifest.keys()
        manifest.pop('upload_time')
        assert manifest == {'uuid': '1234', 'parent_uuids': [], 'basket_type': 'test_basket_type', 'label': ''}
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_manifest_cached(self, patch):
        #upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type)
        
        basket = Basket(self.basket_path)
        
        # manifest should be stored in the 
        # object at this step
        manifest = basket.get_manifest()
        
        manifest_path = f'{self.basket_path}/basket_manifest.json'
        
        # Manually replace the manifest file
        self.fs.rm(manifest_path)
        with self.fs.open(manifest_path, "w") as outfile:
            json.dump({'junk': 'b'}, outfile)
        
        # manifest should already be cached 
        # and the new file shouldn't be read
        manifest = basket.get_manifest()
        assert 'upload_time' in manifest.keys()
        manifest.pop('upload_time')
        assert manifest == {'uuid': '1234', 'parent_uuids': [], 'basket_type': 'test_basket_type', 'label': ''}
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_supplement(self, patch):
        #upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type)
        
        basket = Basket(self.basket_path)
        supplement = basket.get_supplement()
        assert supplement == {'upload_items': upload_items, 'integrity_data': []}
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_supplement_cached(self, patch):
        #upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type)
        
        basket = Basket(self.basket_path)
        
        # Supplement should be stored in the 
        # object at this step
        supplement = basket.get_supplement()
        
        supplement_path = f'{self.basket_path}/basket_supplement.json'
        
        # Manually replace the Supplement file
        self.fs.rm(supplement_path)
        with self.fs.open(supplement_path, "w") as outfile:
            json.dump({'junk': 'b'}, outfile)
        
        # Supplement should already be cached 
        # and the new file shouldn't be read
        supplement = basket.get_supplement()
        assert supplement == {'upload_items': upload_items, 'integrity_data': []}
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_metadata(self, patch):
        #upload basket
        metadata_in = {'test': 1}
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type,
                     metadata = metadata_in)
        
        basket = Basket(self.basket_path)
        metadata = basket.get_metadata()
        assert metadata_in == metadata
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_metadata_cached(self, patch):
        #upload basket
        metadata_in = {'test': 1}
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type,
                     metadata = metadata_in)
        
        basket = Basket(self.basket_path)
        
        # Metadata should be stored in the 
        # object at this step
        metadata = basket.get_metadata()
        
        metadata_path = f'{self.basket_path}/basket_metadata.json'
        
        # Manually replace the metadata file
        self.fs.rm(metadata_path)
        with self.fs.open(metadata_path, "w") as outfile:
            json.dump({'junk': 'b'}, outfile)
        
        # Metadata should already be cached 
        # and the new file shouldn't be read
        metadata = basket.get_metadata()
        assert metadata_in == metadata
        
    @patch('weave.config.get_file_system', return_value=LocalFileSystem())
    def test_basket_get_metadata_none(self, patch):
        #upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(upload_items,
                      self.basket_path, self.uuid, self.basket_type)
        
        basket = Basket(self.basket_path)
        metadata = basket.get_metadata()
        assert metadata == None
