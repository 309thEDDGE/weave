from weave import config
from weave.basket import Basket

def load_mongo(index_table):    
    db = config.get_mongo_db()
    
    for index, row in index_table.iterrows():
        basket = Basket(row['address'])
        metadata = basket.get_metadata()
        manifest = basket.get_manifest()
        mongo_metadata = {}
        mongo_metadata['uuid'] = manifest['uuid']
        mongo_metadata['basket_type'] = manifest['basket_type']
        mongo_metadata.update(metadata)
        db['metadata'].insert_one(mongo_metadata)
