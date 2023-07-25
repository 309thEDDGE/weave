from pathlib import Path
from weave import validate
from tests import test_validate

tv = test_validate.TestValidate()
test_validate.test_validate_invalid_manifest_schema(tv)

