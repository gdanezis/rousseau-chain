.. hippiehug documentation master file, created by
   sphinx-quickstart on Sat Mar  5 23:09:50 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

The hippiehug Merkle Tree Library
=================================
 
Contents:

.. toctree::
   :maxdepth: 2

   index

Installation
------------

The hippiehug Merkle Tree is a pyre python library, available through the usual pypi repositories. You can install it using ``pip``:

.. code-block:: none

  pip install hippiehug

The Redis backend requires a working installation of the Redis database and associated python libraries.

Introduction and Examples
-------------------------

The Merkle Tree Classes
-----------------------

.. autoclass:: hippiehug.Tree
   :members:
   :special-members: __init__

.. autoclass:: hippiehug.Leaf
   :members:

.. autoclass:: hippiehug.Branch
   :members: 

Backend Storage Drivers
-----------------------

.. autoclass:: hippiehug.RedisStore
   :members: 
   :special-members: __init__

Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`

