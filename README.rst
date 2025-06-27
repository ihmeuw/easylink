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

**NOTE: This package requires AMD64 CPU architecture - it is not compatible with
Apple's ARM64 architecture (e.g. M1 and newer Macs).**

There are a few things to install in order to use this package:

- Set up Linux.

  Singularity (and thus EasyLink) requires Linux to run. If you are not already
  using Linux, you will need to set up a virtual machine; refer to the 
  `singularity documentation for installing on Windows or Mac <https://docs.sylabs.io/guides/4.1/admin-guide/installation.html#installation-on-windows-or-mac>`_. 

- Install singularity.

  First check if you already have singularity installed by running the command
  ``singularity --version``. For an existing installation, your singularity version
  number is printed.

  If singularity is not yet installed, you will need to install it;
  refer to the `singularity docs for installing on Linux <https://docs.sylabs.io/guides/4.1/admin-guide/installation.html#installation-on-linux>`_.

  Note that you may need to request installation from your system admin.

- Install conda. 
  
  We recommend `miniforge <https://github.com/conda-forge/miniforge>`_. You can
  check if you already have conda installed by running the command ``conda --version``.
  For an existing installation, a version will be displayed.

- Create a conda environment with python and graphviz installed.

  $ conda create --name easylink -c conda-forge python=3.12 graphviz 'gcc<14' -y
  $ conda activate easylink

  - Install easylink in the environment.

  Option 1 - Install from PyPI with pip::

    $ pip install easylink

  Option 2 - Build from source with pip::
    
    $ pip install git+https://github.com/ihmeuw/easylink.git

.. _end_installation:

Documentation
=============

You can view documentation at https://easylink.readthedocs.io/en/latest/
