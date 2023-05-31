import pymongo
import pandas
from weave.basket import Basket

def load_mongo(index_table):
    client = pymongo.MongoClient('mongodb://localhost:27017/')  
    db = client.mongo_database
    for index, row in index_table.iterrows():
        basket = Basket(row['address'])
        metadata = basket.get_metadata()
        manifest = basket.get_manifest()
        mongo_metadata = {}
        mongo_metadata['uuid'] = manifest['uuid']
        mongo_metadata['basket_type'] = manifest['basket_type']
        mongo_metadata.update(metadata)
        db.metadata.insert_one(mongo_metadata)
    