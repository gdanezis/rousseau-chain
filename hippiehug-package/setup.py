#!/usr/bin/env python

import os
from setuptools import setup


def main():
    with open(os.path.join("hippiehug", "__init__.py")) as f:
        for line in f:
            if "__version__" in line.strip():
                version = line.split("=", 1)[1].strip().strip('"')
                break
        else:
            raise RuntimeError("version not found")

    setup(name='hippiehug',
          version=version,
          description='A Merkle Tree implementation with a flexible storage backend.',
          author='George Danezis',
          author_email='g.danezis@ucl.ac.uk',
          url=r'https://pypi.python.org/pypi/hippiehug/',
          packages=['hippiehug'],
          license="2-clause BSD",
          long_description="""A Merkle Tree and hash chains implementation with a flexible storage backend.""",

          install_requires=[
                "redis >= 2.0.0",
                "future >= 0.14.3",
                "msgpack-python >= 0.4.6",
                "six >= 1.10.0"
          ],
          zip_safe=False,
    )

if __name__ == "__main__":
    main()
