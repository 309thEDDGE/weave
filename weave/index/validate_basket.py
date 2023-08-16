"""
Wherein is contained functionality concerning validating the 
basket_manifest.json file.
"""
import jsonschema
from jsonschema import validate

from ..config import manifest_schema

# validate basket keys and value data types on read in
def validate_basket_dict(basket_dict):
    """validate the basket_manifest.json has the correct structure

    Parameters:
        basket_dict: dictionary read in from basket_manifest.json in minio

    Returns:
        valid (bool): True if basket has correct schema, false otherwise
    """

    try:
        validate(instance=basket_dict, schema=manifest_schema)
        return True

    except jsonschema.exceptions.ValidationError:
        return False
