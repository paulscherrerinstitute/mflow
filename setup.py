#!/usr/bin/env python

import os
from setuptools import find_packages, setup

PACKAGE_PREFIX = "psi-"
PACKAGE_NAME = "mflow"

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name=PACKAGE_PREFIX + PACKAGE_NAME,
    version="1.0.0",
    url="https://github.com/paulscherrerinstitute/" + PACKAGE_NAME,
    description="Utility library for ZMQ data streaming",
    author="Paul Scherrer Institute",
    license="GNU GPLv3",
    install_requires=["numpy", "pyzmq"],
    packages=find_packages(),
    long_description=read('Readme.md'),
    long_description_content_type="text/markdown",

)



