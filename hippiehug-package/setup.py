#!/usr/bin/env python

from setuptools import setup

setup(name='hippiehug',
      version='0.1.2',
      description='A Merkle Tree implementation with a flexible storage backend.',
      author='George Danezis',
      author_email='g.danezis@ucl.ac.uk',
      url=r'https://pypi.python.org/pypi/hippiehug/',
      packages=['hippiehug'],
      license="2-clause BSD",
      long_description="""A Merkle Tree and hash chains implementation with a flexible storage backend.""",

      test_require=["pytest >= 2.6.4"],
      install_requires=[
            "redis >= 2.0.0",
            "future >= 0.14.3",
            "pytest >= 2.6.4",
            "msgpack-python >= 0.4.6",
            "six >= 1.10.0"
      ],
      zip_safe=False,
)
