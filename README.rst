========
EasyLink
========

EasyLink is a framework that allows users to build and run highly configurable
entity resolution (ER) pipelines.

Installation
============

There are a few things to install in order to use this package:

- Install singularity. If this is not already installed on your system, you will likely need to request it from your system admin. Refer to https://docs.sylabs.io/guides/4.1/admin-guide/installation.html

- Install graphviz via
    
    ``> conda install graphviz``

- Install EasyLink via

    ``> pip install easylink``

    OR

    ``> cd <path/to/repositories/>``

    ``> git clone git@github.com:ihmeuw/easylink.git``

    ``> # OR git clone https://github.com/ihmeuw/easylink.git``

    ``> cd easylink``

    ``> pip install .``

Quickstart
==========

To run a pipeline, use `easylink run` from the command line and pass in the
paths to both a pipeline specification and an input data specification:

    ``> easylink run -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION>``

There are several other optional arguments to `easylink run` as well;
for help, use `easylink run --help`

Note that a schematic of the pipeline's directed acyclic graph (DAG) that is run 
is automatically generated. If this schematic is desired _without_ actually
running the pipeline, use `easylink generate-dag`:

    ``> easylink generate-dag -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION>``

As before, refer to `easylink generate-dag --help` for information on other
options.

Requirements
============

TBD

Documentation
=============

TBD
