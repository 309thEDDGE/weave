from pathlib import Path
import json
from weave import config

class Basket():
    def __init__(self, basket_address):
        self.basket_address = Path(basket_address)
        self.manifest_path = str(self.basket_address / 'basket_manifest.json')
        self.supplement_path = str(self.basket_address / 'basket_supplement.json')
        self.metadata_path = str(self.basket_address / 'basket_metadata.json')
        
        self.manifest = None
        self.supplement = None
        self.metadata = None
        
        self.fs = config.get_file_system()
        if not self.fs.exists(basket_address):
            raise ValueError(f'Basket does not exist: {basket_address}')
        
        self.validate_basket()        
        
    def validate_basket(self):        
        if not self.fs.exists(self.manifest_path):
            raise FileNotFoundError(f"Invalid Basket, basket_manifest.json "
                                    f"does not exist: {self.manifest_path}")

        if not self.fs.exists(self.supplement_path):
            raise FileNotFoundError(f"Invalid Basket, basket_supplement.json "
                                    f"does not exist: {self.supplement_path}")
            
    def get_manifest(self):
        with self.fs.open(self.manifest_path, 'rb') as file:
            return json.load(file)
    
    def get_supplement(self):
        with self.fs.open(self.supplement_path, 'rb') as file:
            return json.load(file)
    
    def get_metadata(self):
        if self.fs.exists(self.metadata_path):
            with self.fs.open(self.metadata_path, 'rb') as file:
                return json.load(file)
        else:
            return None

    def ls(self):
        pass