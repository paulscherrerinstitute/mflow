#!/usr/bin/env python

from distutils.core import setup

setup(name='mflow',
      version='0.0.1',
      description='Utility library for ZMQ data streaming',
      author='Paul Scherrer Institute',
      license='GNU GPLv3',
      packages=['mflow', 'mflow.utils', 'mflow.handlers'],
     )