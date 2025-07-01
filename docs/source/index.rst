========
EasyLink
========

.. include:: ../../README.rst
   :start-after: .. _intro:
   :end-before: .. _end_intro:

Installation
============

.. include:: ../../README.rst
   :start-after: .. _python_support:
   :end-before: .. _end_python_support:

.. include:: ../../README.rst 
   :start-after: .. _installation:
   :end-before: .. _end_installation:

Once you have EasyLink installed, see the :ref:`getting_started` tutorial for how to use it.

Motivation
==========

Imagine the Census Bureau has a record linkage pipeline that links people between datasets.
One step in this pipeline, called "blocking," categorizes records into "blocks"
in order to focus only on the pairs of records that might really be links.
The current pipeline uses a simple blocking mechanism,
which won't compare two records unless they match exactly on any of a few key attributes.
Census wants to explore whether using more sophisticated blocking methods would improve results,
without changing anything else in the pipeline.

Currently, software for record linkage is mostly created by researchers.
Each researcher uses the technologies familiar to them and frames the record linkage task
in the way that is most natural for their own examples,
making it hard to use multiple software modules together.
As a result, trying a new blocking method is too expensive for the Census Bureau
to undertake without knowing what the benefit will be.

EasyLink aims to solve this problem by standardizing the record linkage pipeline and providing an
"ecosystem" of compatible record linkage implementations.
With EasyLink, a switch from one software to another requires only a change to a configuration file.

.. toctree::
   :hidden:
   :maxdepth: 2

   tutorial/index
   concepts/index
   api_reference/index
   glossary
