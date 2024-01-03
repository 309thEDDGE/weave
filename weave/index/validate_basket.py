"""Wherein is contained functionality concerning validating the
basket_manifest.json file.
"""

import jsonschema
from jsonschema import validate

from ..config import manifest_schema


# Validate basket keys and value data types on read in
def validate_basket_dict(basket_dict):
    """Validate the basket_manifest.json has the correct structure.

    Parameters:
    ----------
    basket_dict: dict
        Dictionary of the basket contents
        read in from basket_manifest.json in the file system.

    Returns
    ----------
    bool: True if basket has correct schema, False otherwise.
    """

    try:
        validate(instance=basket_dict, schema=manifest_schema)
        return True

    except jsonschema.exceptions.ValidationError:
        return False
