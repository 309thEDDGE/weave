from setuptools import setup

setup(
    name="weave",
    version="0.0.1",
    packages=["weave"],
    install_requires=["fsspec", "pandas", "s3fs"],
)
