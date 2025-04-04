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

- Install singularity. 

  You may need to request it from your system admin. 
  Refer to https://docs.sylabs.io/guides/4.1/admin-guide/installation.html. 
  You can check if you already have singularity installed by running the command ``singularity --version``. For an
  existing installation, your singularity version number is printed.

- Install conda. 
  
  We recommend `miniforge <https://github.com/conda-forge/miniforge>`_. You can check if you already
  have conda installed by running the command ``conda list``. For an existing installation, a list of installed 
  packages appears.

- Install easylink, python and graphviz in a conda environment.

  Option 1 - Install from PyPI with pip::

    $ conda create --name easylink python=3.12 graphviz -y
    $ conda activate easylink
    $ pip install easylink

  Option 2 - Build from source with pip::
    
    $ conda create --name easylink python=3.12 graphviz -y
    $ conda activate easylink
    $ git clone git@github.com:ihmeuw/easylink.git  # or git clone https://github.com/ihmeuw/easylink.git
    $ cd easylink
    $ pip install .

.. _end_installation:

Documentation
=============

You can view documentation at https://easylink.readthedocs.io/en/latest/
