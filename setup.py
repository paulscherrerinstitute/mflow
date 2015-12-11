#!/usr/bin/env python

from distutils.core import setup

setup(name='mflow',
      version='0.0.1',
      description='Python Library for ZMQ data streaming',
      author='Paul Scherrer Institute',
      packages=['mflow', 'mflow.utils', 'mflow.handlers'],
     )