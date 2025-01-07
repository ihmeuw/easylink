========
EasyLink
========

EasyLink is a framework that allows users to build and run highly configurable
entity resolution (ER) pipelines.

.. _python_support:

**Supported Python versions: 3.11, 3.12**

.. _end_python_support:

Installation
============

.. _installation:

There are a few things to install in order to use this package:

- Install singularity. If this is not already installed on your system, you will 
  likely need to request it from your system admin. 
  Refer to https://docs.sylabs.io/guides/4.1/admin-guide/installation.html


- Install graphviz via:

  .. code-block:: console

    $ conda install graphviz

- Install EasyLink.

  Option 1 - Install from PyPI with pip::

    $ pip install easylink

  Option 2 - Build from source with pip::

    $ git clone git@github.com:ihmeuw/easylink.git  # or git clone https://github.com/ihmeuw/easylink.git
    $ cd easylink
    $ pip install .

.. _end_installation:

Documentation
=============

You can view documentation at https://easylink.readthedocs.io/en/latest/
