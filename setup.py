__author__ = "aman.rv"

from setuptools import find_packages, setup

setup(
    name="transformer",
    version="0.0.1",
    packages=find_packages(exclude=("tests",)),
    install_requires=["pyhocon==0.3.60"],
    zip_safe=False,
)
