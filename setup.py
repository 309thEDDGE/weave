from setuptools import setup

setup(
    name="weave",
    version="0.8.1",
    packages=["weave", "weave/tests", "weave/index"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo", "jsonschema"]
)
