from setuptools import setup

setup(
    name="weave",
    version="0.0.1",
    packages=["weave"],
    install_requires=["boto3", "fsspec", "pandas", "s3fs"],
)
