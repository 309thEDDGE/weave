from setuptools import setup

setup(
    name="weave",
    version="0.10.0",
    packages=["weave", "weave/tests", "weave/index"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo", "jsonschema"]
)
