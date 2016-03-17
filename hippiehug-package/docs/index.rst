.. hippiehug documentation master file, created by
   sphinx-quickstart on Sat Mar  5 23:09:50 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

The hippiehug Merkle Tree Library
=================================
 
Installation
------------

The hippiehug Merkle Tree is a pure python library, available through the usual pypi repositories. You can install it using ``pip``:

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

Security Properties
-------------------

The key security property offered by the hippiehug Tree relates to *high-integrity*, despite a possibly adversarial store. Given the root value of the tree is kept with high-integrity, the integrity of the addition (``add``) and set membership (``is_in``) operations, are guaranteed to be correct if they return a result. 

However, *availability* properties are not guaranteed: a store that does not respond, loses or modifies data can make operations fail. Replicating the store across different parties can mitigate this.

The Merkle Tree Classes
-----------------------

.. autoclass:: hippiehug.Tree
   :members:
   :special-members: __init__

.. autoclass:: hippiehug.Chain
   :members:
   :special-members: __init__

Helper Structures
-----------------

.. autoclass:: hippiehug.Leaf
   :members:

.. autoclass:: hippiehug.Branch
   :members: 

.. autoclass:: hippiehug.Block
   :members:

Backend Storage Drivers
-----------------------

.. autoclass:: hippiehug.RedisStore
   :members: 
   :special-members: __init__

Development and How to Contribute?
----------------------------------

The development of ``hippiehug`` takes place on github_. Please send patches and bug fixes through pull requests. You can clone the repository, and test the package, through the following commands:

.. code-block:: none

  git clone https://github.com/gdanezis/rousseau-chain.git
  cd hippiehug-package
  paver test

Other targets for paver include ``docs`` to build documentation, and ``build`` to build the package ready for pip installation or distribution.

.. _github: https://github.com/gdanezis/rousseau-chain



Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`

