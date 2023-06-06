import pandas as pd
from weave import config, Basket

def load_mongo(index_table):
    
    if not isinstance(index_table, pd.DataFrame):
        raise TypeError("Invalid datatype for index_table: "
                        "must be Pandas DataFrame")
    
    required_columns = ['uuid', 'basket_type', 'address']
    
    for required_column in required_columns:
        if required_column not in index_table.columns.values.tolist():
            raise ValueError("Invalid index_table: missing "
                             f"{required_column} column")
    
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
