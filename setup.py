#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

if sys.version_info[:2] < (2, 7):
    sys.exit("ERROR: Python >= 2.7 required")

if sys.version_info[0] > 2:
    sys.exit("ERROR: Python < 3.0 required")

from distutils.core import setup

setup(
    name='pymar',
    version="0.1.1",
    description="Tool for distributed computing with python",
    url="https://github.com/alexgorin/pymar",
    author="Alexander Gorin",
    author_email='saniagorin@gmail.com',
    license='MIT',
    packages=[
        'pymar',
        'pymar.tests'
    ],
    scripts=[
        'worker.py'
    ],
    install_requires=[
          'pika',
    ],
    keywords=[
        'python', 'scale', 'distribute', 'map', 'reduce', 'mongo'
    ]
)