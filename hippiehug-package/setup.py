#!/usr/bin/env python

from setuptools import setup

import hippiehug

setup(name='hippiehug',
      version=hippiehug.VERSION,
      description='A Merkle Tree implementation with a flexible storage backend.',
      author='George Danezis',
      author_email='g.danezis@ucl.ac.uk',
      url=r'https://pypi.python.org/pypi/hippiehug/',
      packages=['hippiehug'],
      license="2-clause BSD",
      long_description="""A Merkle Tree implementation with a flexible storage backend.""",

      setup_requires=["pytest >= 2.6.4"],
      tests_require=[
            "redis  >= 2.0.0",
            "future >= 0.14.3",
            "pytest >= 2.6.4",
            "msgpack-python >= 0.4.6",
      ],
      install_requires=[
            "redis >= 2.0.0",
            "future >= 0.14.3",
            "pytest >= 2.6.4",
            "msgpack-python >= 0.4.6",
      ],
      zip_safe=False,
)