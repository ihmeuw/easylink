======================
EasyLink Documentation
======================

.. todo::

    - Summary
    - Strengthen quickstart examples

EasyLink is a framework for ...

Quickstart
==========

.. include:: ../../README.rst
   :start-after: .. _python_support:
   :end-before: .. _end_python_support:

.. include:: ../../README.rst 
   :start-after: .. _installation:
   :end-before: .. _end_installation:

Once installed, you can run a pipeline by typing ``easylink run`` into the command 
line and passing in the paths to both a pipeline specification and an input data 
specification:

.. highlight:: console

::

   $ easylink run -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION>

There are several other optional arguments to ``easylink run`` as well;
for help, use ``easylink run --help``.

Note that a schematic of the pipeline's directed acyclic graph (DAG) that is run 
is automatically generated. If this schematic is desired _without_ actually
running the pipeline, use ``easylink generate-dag``:

::

   $ easylink generate-dag -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION

As before, refer to ``easylink generate-dag --help`` for information on other options.

.. toctree::
   :maxdepth: 2

   user_guide/index
   concepts/index
   api_reference/index
   glossary
