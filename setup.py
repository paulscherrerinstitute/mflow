#!/usr/bin/env python

from setuptools import find_packages, setup


setup(
    name="mflow",
    version="0.2.0",
    url="https://github.com/paulscherrerinstitute/mflow",
    description="Utility library for ZMQ data streaming",
    author="Paul Scherrer Institute",
    license="GNU GPLv3",
    requires=["pyzmq"],
    packages=find_packages()
)



