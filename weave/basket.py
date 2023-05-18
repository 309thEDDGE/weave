from pathlib import Path
import json
import os
from weave import config

class Basket():
    def __init__(self, basket_address):
        self.basket_address = basket_address
        self.manifest_path = f'{self.basket_address}/basket_manifest.json'
        self.supplement_path = f'{self.basket_address}/basket_supplement.json'
        self.metadata_path = f'{self.basket_address}/basket_metadata.json'
        self.manifest = None
        self.supplement = None
        self.metadata = None
        self.fs = config.get_file_system()
        self.validate()
        
    def validate(self):
        if not isinstance(self.basket_address, str):
            raise TypeError(f"Basket address must be a string: {str(self.basket_address)}")
        
        if not self.fs.exists(self.basket_address):
            raise ValueError(f'Basket does not exist: {self.basket_address}')
            
        if not self.fs.exists(self.manifest_path):
            raise FileNotFoundError(f"Invalid Basket, basket_manifest.json "
                                    f"does not exist: {self.manifest_path}")

        if not self.fs.exists(self.supplement_path):
            raise FileNotFoundError(f"Invalid Basket, basket_supplement.json "
                                    f"does not exist: {self.supplement_path}")
            
    def get_manifest(self):
        if self.manifest != None:
            return self.manifest
        
        with self.fs.open(self.manifest_path, 'rb') as file:
            self.manifest = json.load(file)
            return self.manifest
    
    def get_supplement(self):
        if self.supplement != None:
            return self.supplement
        
        with self.fs.open(self.supplement_path, 'rb') as file:
            self.supplement = json.load(file)
            return self.supplement
    
    def get_metadata(self):
        if self.metadata != None:
            return self.metadata
        
        if self.fs.exists(self.metadata_path):
            with self.fs.open(self.metadata_path, 'rb') as file:
                self.metadata = json.load(file)
                return self.metadata
        else:
            return None

    def ls(self, relative_path = None):        
        ls_path = self.basket_address
        if relative_path != None:
            if not isinstance(relative_path, str):
                raise TypeError(f"Invalid type for relative_path: "
                                f"got {type(relative_path)} expected str")
            ls_path = os.path.join(ls_path, relative_path)
        ls_results = self.fs.ls(ls_path)
        ls_results = [x for x in ls_results if os.path.basename(Path(x))
                      not in config.prohibited_filenames]
        return ls_results