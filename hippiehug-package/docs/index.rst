.. hippiehug documentation master file, created by
   sphinx-quickstart on Sat Mar  5 23:09:50 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

The hippiehug Merkle Tree Library
=================================
 
Installation
------------

The hippiehug Merkle Tree is a pyre python library, available through the usual pypi repositories. You can install it using ``pip``:

.. code-block:: none

  pip install hippiehug

The Redis backend requires a working installation of the Redis database and associated python libraries.

Introduction and Examples
-------------------------

The hippiehug module provides a *Merkle Tree* data structure representing a set of byte strings. 

Using secure hash functions the tree is stored into a data store, that may be persistent and remote. The store is not trusted for integrity, however though cryptographic checks membership operations for items in the set are guaranteed to return the correct answer (or no answer).

In the following example we create a new ``Tree``, and insert *item* ``Hello`` into it. Subsequent queries for items ``Hello`` and ``World`` return the expected results.

.. literalinclude:: ../tests/test_doc.py
   :language: python
   :lines: 2-6

Merkle Trees also allow us to extract a small amount of evidence that would convince anyone with knowledge of the root of the tree whether an element is is the represented set, or not. The volume of this evidence is logarithmic in the size of the set, and therefore efficient.

In the following example we extract information out of a tree, and then check it by reconstructing a partial tree with the evidence.

.. literalinclude:: ../tests/test_doc.py
   :language: python
   :lines: 9-17

A number of stores can be used to back the state of the tree. Those can be local or remote. By default a local python dictionary is used, which offers no persistence. However, the library also offers a Redis backed store through ``RedisStore``. Any class that defined ``__getitem__`` and ``__setitem__`` may be used as a store. Neither its integrity, not its consistency can affect the integrity of the set operations on the Tree.

.. literalinclude:: ../tests/test_doc.py
   :language: python
   :lines: 24-29


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

