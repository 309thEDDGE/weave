import os
from setuptools import setup

def read(rel_path: str) -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    # intentionally *not* adding an encoding option to open.
    # See: https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with open(os.path.join(here, rel_path)) as file_path:
        return file_path.read()


def get_version(rel_path: str) -> str:
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


setup(
    name="weave",
    version=get_version("weave/__init__.py"),
    packages=["weave", "weave/tests", "weave/index"],
    install_requires=["pandas", "s3fs", "fsspec", "jsonschema"],
    extras_require={
        "extras": ["pymongo", "pyodbc"],
    }
)
