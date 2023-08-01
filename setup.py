from setuptools import setup

setup(
    name="weave",
    version="0.5.2",
    packages=["weave", "weave/tests"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo", "jsonschema"]
)
