from weave import file_system

'''
config.py provides configuration settings used by weave.
'''
# As schema change, a parameter can be passed to index_schema
# to determine which schema to return.
def index_schema():
    '''
    Return the keys expected from the manifest.json file.
    '''
    return ["uuid", "upload_time", "parent_uuids", "basket_type", "label"]

def get_file_system():
    return file_system