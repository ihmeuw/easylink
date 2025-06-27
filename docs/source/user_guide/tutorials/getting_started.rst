.. _getting_started:

===============
Getting Started
===============

Introduction
============
EasyLink is a framework that allows users to build and run highly configurable record linkage pipelines. 
Its configurability enables users to "mix and match" different pieces of record 
linkage software by ensuring that each piece of the pipeline conforms to standard patterns. 

For example, users at the Census Bureau could easily evaluate whether using a more sophisticated "blocking" 
method would improve results in a certain pipeline, without having to rewrite the entire pipeline. For more 
information, read about the `motivation <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#motivation>`_
behind EasyLink.

Overview
--------
This tutorial introduces EasyLink concepts and features by demonstrating the software's usage. Covered 
concepts include the EasyLink record linkage "pipeline schema", Easylink pipeline configuration, running 
pipelines, changing record linkage step implementations, changing input data, evaluating and comparing 
results, and more. 

.. contents::

Audience
--------
This tutorial is intended for people familiar with record linkage practices, who are interested
in easily comparing linkage results across different methods. This tutorial will *not* cover information 
about linkage techniques or software and assumes the reader is familiar with these concepts and 
ready to implement that knowledge using EasyLink.

Tutorial prerequisites
----------------------
`Install EasyLink <https://github.com/ihmeuw/easylink?tab=readme-ov-file#installation>`_ if you haven't already. 

The tutorial uses the `Splink <https://moj-analytical-services.github.io/splink/index.html>`_ Python package 
for record linkage implementations, which is included when you install EasyLink. Splink knowledge is not 
required to complete the tutorial but may be helpful when configuring Splink models.


Simulated input data
--------------------
Our first demonstration of running an EasyLink pipeline will configure a simple, "naive" record linkage
model using Splink implementations. Our pipeline will link the
two of the `pseudopeople <https://pseudopeople.readthedocs.io/en/latest/>`_
simulated `datasets <https://pseudopeople.readthedocs.io/en/latest/datasets/index.html>`_:
``Social Security Administration`` and ``Tax forms: W-2 & 1099``.


Naive model - running a pipeline
================================
Let's start by using the ``easylink run`` :ref:`command <cli>` to run a pipeline that configures a simple 
record linkage model.

First we need to download the input files we will pass to the command: 
:download:`environment_local.yaml <environment_local.yaml>`, 
:download:`input_data_demo.yaml`, and :download:`pipeline_demo_naive.yaml`. Save them to the directory
from which you will execute the ``easylink run`` command. 

``input_data_demo.yaml`` additionally references a few 
input files which we will save as well. Save :download:`known_clusters.parquet` to the same directory as
the other files, then create a ``/2020`` directory and save :download:`input_file_ssa.parquet <2020/input_file_ssa.parquet>` and 
:download:`input_file_w2.parquet <2020/input_file_w2.parquet>` to it.

Now we can run the pipeline. Note that if this is your first time running EasyLink, the command will first
download the required `singularity <https://docs.sylabs.io/guides/latest/user-guide/introduction.html>`_`  
container images. These files contain the code EasyLink will run for each step in the record linkage 
pipeline. The total amount to be downloaded is approximately 5GB, so we recommend first running the command 
below, then reading the information about it while the files download. Hopefully the download will be 
complete by the time you reach the next interactive section! The progress of your image downloads will be 
displayed in the console.

.. code-block:: console

    $ easylink run -p pipeline_demo_naive.yaml -i input_data_demo.yaml -e environment_local.yaml -I /mnt/team/simulation_science/priv/engineering/er_ecosystem/images
  2025-06-26 10:13:31.501 | 0:00:01.693505 | run:196 - Running pipeline
  2025-06-26 10:13:31.502 | 0:00:01.693704 | run:198 - Results directory: /mnt/share/homes/tylerdy/easylink/docs/source/user_guide/tutorials/results/2025_06_26_10_13_31
  2025-06-26 10:13:52.719 | 0:00:22.911314 | main:124 - Running Snakemake
  [Thu Jun 26 10:13:53 2025]
  Job 14: Validating determining_exclusions_and_removing_records_clone_1_removing_records_default_removing_records input slot input_datasets
  Reason: Missing output files: input_validations/determining_exclusions_and_removing_records_clone_1_removing_records_default_removing_records/input_datasets_validator
  ...
  [Thu Jun 26 10:13:58 2025]
  Job 28: Validating splink_evaluating_pairs input slot known_links
  Reason: Missing output files: input_validations/splink_evaluating_pairs/known_links_validator; Input files updated by another job: intermediate/default_clusters_to_links/result.parquet
  ...
  [Thu Jun 26 10:14:47 2025]
  Job 1: Running canonicalizing_and_downstream_analysis implementation: dummy_canonicalizing_and_downstream_analysis
  Reason: Missing output files: intermediate/dummy_canonicalizing_and_downstream_analysis/result.parquet; Input files updated by another job: input_validations/dummy_canonicalizing_and_downstream_analysis/input_datasets_validator, intermediate/default_updating_clusters/clusters.parquet, input_validations/dummy_canonicalizing_and_downstream_analysis/clusters_validator
  [Thu Jun 26 10:14:50 2025]
  Job 35: Validating results input slot analysis_output
  Reason: Missing output files: input_validations/final_validator; Input files updated by another job: intermediate/dummy_canonicalizing_and_downstream_analysis/result.parquet
  [Thu Jun 26 10:14:51 2025]
  Job 0: Grabbing final output
  Reason: Missing output files: result.parquet; Input files updated by another job: input_validations/final_validator, intermediate/dummy_canonicalizing_and_downstream_analysis/result.parquet

Success! Our pipeline has linked the input data and outputted the results, the clusters of records it found. We'll take a look 
at these results later and see how the model performed. But first we will explore each of the arguments we 
passed to the command.

.. note:: 
   The pipeline output in its current state can be a little confusing. Note that the number assigned 
   to the slurm jobs is different than the order the jobs are executed in - these job IDs are 
   assigned by `Snakemake <https://snakemake.readthedocs.io/en/stable/>`_, a workflow manager for reproducible,
   scalable data analyses. Also note that several input validation jobs will run before any actual 
   step implementations.

   Finally, despite the final output line containing the phrase "Missing output files", 
   this pipeline finished executing successfully. The "Reason" displayed in the output is explaining 
   why the job was run (the step inputs were ready but the output file did not yet exist), rather than 
   conveying an error message. We plan to improve these error messages in the future.

Naive model - command line arguments
====================================

Computing Environment
---------------------
The ``--computing-environment`` (``-e``) argument to ``easylink run`` accepts a YAML file specifying 
information about the computing environment which will execute the steps of the 
pipeline. We passed ``environment_local.yaml``, the contents of which are shown below::

   computing_environment: local
   container_engine: singularity

It specifies a ``local`` computing environment using ``singularity`` as the container engine. These parameters indicate that no new compute resources will 
be used to execute the pipeline steps, and that the Singularity container for each implementation will run within the context where ``easylink run`` is being executed.
For example, if you ran the ``easylink run`` command on your laptop, the implementations would run on your laptop;
if you ran the ``easylink run`` command on a cloud (e.g. EC2) instance that you were connected to with SSH, the implementations would run on that instance,
and so on.

Input data
----------
The ``--input-data`` (``-i``) argument to ``easylink run`` accepts a YAML file specifying a list 
of paths to files or directories containing input data to be used by the pipeline. 
We passed ``input_data_demo.yaml``, the contents of which are shown below::

  input_file_ssa: 2020/input_file_ssa.parquet
  input_file_w2: 2020/input_file_w2.parquet
  known_clusters: known_clusters.parquet

Here we have defined the locations of the three input files we will use: the 2020 versions of the 
``Social Security Administration`` and ``W2 & 1099`` datasets, and an empty ``known_clusters`` file, since no
clusters are known to us before running this pipeline. 

.. note::
    To meet the input specifications for :ref:`datasets` defined by the pipeline schema (see the next section),
    the ``SSA`` and ``W2`` datasets, after being generated by pseudopeople, were modified
    to add the required ``Record ID`` column. ``SSA`` death records were also removed, 
    leaving only ``creation`` type records.
  

Pipeline specification
----------------------
The ``--pipeline-specification`` (``-p``) argument to ``easylink run`` accepts a YAML file specifying 
the implementations and other configuration options for the pipeline being run. We passed 
``pipeline_demo_naive.yaml``, the contents of which can be seen by clicking below:

.. raw:: html

   <details>
   <summary>Show pipeline_demo_naive.yaml</summary>

.. code-block:: yaml

  steps:
      entity_resolution:
        substeps:
          determining_exclusions_and_removing_records:
            clones:
              - determining_exclusions:
                  implementation:
                    name: default_determining_exclusions
                    configuration:
                      INPUT_DATASET: input_file_ssa
                removing_records:
                  implementation:
                    name: default_removing_records
                    configuration:
                      INPUT_DATASET: input_file_ssa
              - determining_exclusions:
                  implementation:
                    name: default_determining_exclusions
                    configuration:
                      INPUT_DATASET: input_file_w2
                removing_records:
                  implementation:
                    name: default_removing_records
                    configuration:
                      INPUT_DATASET: input_file_w2
          clustering:
            substeps:
              clusters_to_links:
                implementation:
                  name: default_clusters_to_links
              linking:
                substeps:
                  pre-processing:
                    clones:
                    - implementation:
                        name: middle_name_to_initial
                        configuration: 
                          INPUT_DATASET: input_file_ssa
                    - implementation:
                        name: dummy_pre-processing
                        configuration: 
                          INPUT_DATASET: input_file_w2
                  schema_alignment:
                    implementation:
                      name: default_schema_alignment
                  blocking_and_filtering:
                    implementation:
                      name: splink_blocking_and_filtering
                      configuration:
                        LINK_ONLY: true
                        BLOCKING_RULES: "'l.first_name == r.first_name,l.last_name == r.last_name'"
                  evaluating_pairs:
                    implementation:
                      name: splink_evaluating_pairs
                      configuration:
                        LINK_ONLY: true
                        BLOCKING_RULES_FOR_TRAINING: "'l.first_name == r.first_name,l.last_name == r.last_name'"
                        COMPARISONS: "'ssn:exact,first_name:exact,middle_initial:exact,last_name:exact'"
                        PROBABILITY_TWO_RANDOM_RECORDS_MATCH: 0.0001  # == 1 / len(w2)
              links_to_clusters:
                implementation:
                  name: splink_links_to_clusters
                  configuration:
                    THRESHOLD_MATCH_PROBABILITY: 0.996
          updating_clusters:
            implementation:
              name: default_updating_clusters
      canonicalizing_and_downstream_analysis:
        implementation:
          name: dummy_canonicalizing_and_downstream_analysis

.. raw:: html

  </details>

The pipeline specification follows the structure defined in the :ref:`pipeline_schema`, a very important
part of EasyLink. The EasyLink pipeline **schema** enforces the standard patterns that linkage step implementations must 
follow, enabling easy configuration and swapping. 

It defines the steps of the record linkage pipeline, the inputs and outputs for each step, and the required formats for 
each input or output data file. 

It also describes a set of operators which are used by the schema to allow customization 
of step behavior, such as :ref:`cloneable_sections`, which create multiple copies of that section and allow different 
implementations or inputs to be specified for each copy. We'll see one of those soon.

.. important::

  Stop! Before proceeding, it's critical to make sure you understand the relationship between a pipeline, a pipeline 
  specification (YAML file), and the pipeline schema:

  - A `pipeline <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#pipelines>`_ 
    consists of a complete set of software which can perform a whole record linkage task, taking in record datasets as inputs and outputting 
    a result such as clusters of records or some analysis on those clusters. EasyLink makes it simple to define and run 
    many different pipelines in order to experiment with what methods yield the best results for a task.
  - A pipeline specification is a YAML file, which defines a pipeline which can be run with EasyLink. It defines the 
    implementation which will be run for each step, and performs any necessary configuration for those implementations. An 
    example specification is expandable above.
  - The EasyLink :ref:`pipeline_schema` defines the universe of pipelines that can be constructed using EasyLink, including
    steps, inputs and outputs, and operators, as described above. All pipelines must adhere to the pipeline schema! 

Top-level steps
^^^^^^^^^^^^^^^

Let's take a closer look at the pipeline specification YAML bit by bit. We'll start at the top level::

  steps:
    entity_resolution:
      substeps:
        ...
    canonicalizing_and_downstream_analysis:
      implementation:
        name: save_clusters

This code block shows the same file, but with all the substeps of ``entity_resolution`` hidden, 
like in `this diagram <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#easylink-pipeline-schema>`_
of the pipeline schema. 

The children of the ``steps`` key are the top-level steps in the pipeline - as you can see, there are 
only two. We can see our first example of a step being configured if we look at ``canonicalizing_and_downstream_analysis``. 
The children of the ``implementation`` key define and configure the code we will run for 
`the step <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#canonicalizing-and-downstream-analysis>`_.
We use the ``name`` key to choose to run the ``save_clusters`` implementation of ``canonicalization_and_downstream_analysis``.
``save_clusters`` corresponds to one of the images which was downloaded the first time you ran the pipeline. The image contains code 
which will simply save the clusters which are inputted into the step (see the diagram linked above) to disk. 

Entity resolution substeps
^^^^^^^^^^^^^^^^^^^^^^^^^^

Next we will show the ellipsed part of the above code block, which corresponds to 
`this diagram <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#entity-resolution-sub-steps>`_
in the pipeline schema::

  determining_exclusions_and_removing_records:
    clones:
      - determining_exclusions:
          implementation:
            name: default_determining_exclusions
            configuration:
              INPUT_DATASET: input_file_ssa
        removing_records:
          implementation:
            name: default_removing_records
            configuration:
              INPUT_DATASET: input_file_ssa
      - determining_exclusions:
          implementation:
            name: default_determining_exclusions
            configuration:
              INPUT_DATASET: input_file_w2
        removing_records:
          implementation:
            name: default_removing_records
            configuration:
              INPUT_DATASET: input_file_w2
  clustering:
    substeps:
      ...
  updating_clusters:
    implementation:
      name: default_updating_clusters

The last step shown, ``updating_clusters``, looks similar to ``canonicalization_and_downstream_analysis`` above; it simply chooses 
an implementation for the step using the ``name`` key. The substeps of ``clustering`` are hidden -- we'll look at them next. 

The complicated part is ``determining_exclusions_and_removing_records`` and its ``clones`` key:

The schema can define steps as :ref:`cloneable_sections`, which create 
multiple copies of that section and allow different implementations or inputs to be defined 
for each copy. We can see that the :ref:`entity_resolution_sub_steps` schema section defines
``determining_exclusions`` and ``removing_records`` as cloneable in the diagram 
(blue dashed box).

In the YAML, the superstep ``determining_exclusions_and_removing_records`` is marked as 
clonable using the ``clones`` key, and two copies are made of its substeps, 
``determining_exclusions`` and ``removing_records``. The ``-`` denotes the beginning
of each of the two copies, each of which must contain both of the substeps. 

We can see that the only difference between the two copies is what filename is passed 
to the ``INPUT_DATASET`` environment variables for each step. In 
the first copy, the ``ssa`` dataset files are used as inputs for both steps, 
while in the second copy, the ``w2`` dataset files are the inputs. In practice, 
this means that records to exclude will be identified and removed separately for 
each input file, as required by the schema since each input file has different data. 
This cloneable section also allows different implementations to be used for each dataset 
if desired.

.. note::
  All the steps listed here use ``default`` implementations. Default implementations generally just pass their input directly to their 
  output without changing it. The behavior of each of these default steps is described in the pipeline schema section linked above the 
  code block.

Clustering substeps
^^^^^^^^^^^^^^^^^^^

Next we will show the ellipsed part of the above code block, which corresponds to 
`this diagram <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#clustering-sub-steps>`_
in the pipeline schema::

  clusters_to_links:
    implementation:
      name: default_clusters_to_links
  linking:
    substeps:
      ...
  links_to_clusters:
    implementation:
      name: splink_links_to_clusters
      configuration:
        THRESHOLD_MATCH_PROBABILITY: 0.996

We will show the hidden linking substeps in the next section. 

In ``links_to_clusters`` we see our first example of configuring an implementation. The children of the ``configuration`` key are 
implementation-specific variables which control how the implementation will run. 

``THRESHOLD_MATCH_PROBABILITY`` here allows the user to define at what probability a pair of records being considered 
as a pontential link will be considered part of the same cluster by ``splink_links_to_clusters``, which uses the Splink package to 
implement the ``links_to_clusters`` `step <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#links-to-clusters>`_.
The Splink docs have
`more info <https://moj-analytical-services.github.io/splink/topic_guides/evaluation/edge_overview.html#choosing-a-threshold>`_ on the 
``THRESHOLD_MATCH_PROBABILITY`` variable.

Linking substeps
^^^^^^^^^^^^^^^^

Next we will show the ellipsed part of the above code block, which corresponds to 
`this diagram <https://easylink.readthedocs.io/en/latest/concepts/pipeline_schema/index.html#linking-sub-steps>`_
in the pipeline schema::

  pre-processing:
    clones:
    - implementation:
        name: middle_name_to_initial
        configuration: 
          INPUT_DATASET: input_file_ssa
    - implementation:
        name: no_pre-processing
        configuration: 
          INPUT_DATASET: input_file_w2
  schema_alignment:
    implementation:
      name: default_schema_alignment
  blocking_and_filtering:
    implementation:
      name: splink_blocking_and_filtering
      configuration:
        LINK_ONLY: true
        BLOCKING_RULES: "l.first_name == r.first_name,l.last_name == r.last_name"
  evaluating_pairs:
    implementation:
      name: splink_evaluating_pairs
      configuration:
        LINK_ONLY: true
        BLOCKING_RULES_FOR_TRAINING: "l.first_name == r.first_name,l.last_name == r.last_name"
        COMPARISONS: "ssn:exact,first_name:exact,middle_initial:exact,last_name:exact"
        PROBABILITY_TWO_RANDOM_RECORDS_MATCH: 0.0001  # == 1 / len(w2)

We see that ``pre-processing`` is another cloneable step, allowing us to select different pre-processing implementations for different
input datasets. In this case, we leave the ``w2`` dataset unchanged, while changing the ``middle_name`` column in the ``ssa`` dataset 
to a ``middle_initial`` column to match ``w2``.



Structure
^^^^^^^^^

Let's break down the configuration keys and values defined in the file. 
First, note that all of the keys defined as direct children of a ``steps`` 
or ``substeps`` key represent record linkage steps from the 
:ref:`pipeline_schema`. They are nested in the same structure defined in 
that document. For example, :ref:`linking_sub_steps` and the ``linking`` YAML 
key both list the same substeps -- ``pre-processing``, 
``schema_alignment``, ``blocking_and_filtering``, and ``evaluating_pairs``.

Now that we understand the nested step structure of the pipeline specification 
YAML, let's discuss the keys used to configure individual steps.

.. _implementation_configuration:

Implementation configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We can see in the YAML that many steps use all three of the ``implementation``, ``name`` 
and ``configuration`` keys, as well as implementation-specific keys. Let's look at 
``links_to_clusters`` as an example.

The ``implementation`` section simply indicates that the subkeys that follow define the
step's implementation in this pipeline.

The ``name`` key selects which of the available implementations for this step will 
be used.

.. todo:: 
    Link to docs for "available implementations" for each step when that is available.

The ``configuration`` section lists implementation-specific configuration keys
which control how the implementation will run. For example, ``THRESHOLD_MATCH_PROBABILITY`` 
here allows the user to define at what probability a pair of records being considered 
as a pontential link will be considered part of the same cluster by the 
``splink_links_to_clusters`` implementation. The Splink docs have 
`more info <https://moj-analytical-services.github.io/splink/topic_guides/evaluation/edge_overview.html#choosing-a-threshold>`_.

Cloneable Sections
^^^^^^^^^^^^^^^^^^
Certain sections of the pipeline are defined as as :ref:`cloneable_sections`, which create 
multiple copies of that section and allow different implementations or inputs to be defined 
for each copy. We can see that :ref:`entity_resolution_sub_steps` defines
``determining_exclusions`` and ``removing_records`` as cloneable in the diagram 
(blue dashed box).

In the YAML, the superstep ``determining_exclusions_and_removing_records`` is marked as 
clonable using the ``clones`` key, and two copies are made of its substeps, 
``determining_exclusions`` and ``removing_records``. The ``-`` denotes the beginning
of each of the two copies, each of which must contain both of the substeps. 

We can see that the only difference between the two copies is what filename is passed 
to the ``INPUT_DATASET`` environment variables for each step. In 
the first copy, the ``ssa`` dataset files are used as inputs for both steps, 
while in the second copy, the ``w2`` dataset files are the inputs. In practice, 
this means that records to exclude will be identified and removed separately for 
each input file, as required by the schema since each input file has different data. 
This cloneable section also allows different implementations to be used for each dataset 
if desired.

Naive model - configuring Splink
================================
Having explained how the inputs, general pipeline format, and computing environment
are specified, now we will discuss how the pipeline specification configures 
our actual Splink record linkage model.

There are three Splink implementations in the pipeline specification YAML 
for us to configure: ``splink_blocking_and_filtering``, ``splink_evaluating_pairs``,
and ``splink_links_to_clusters``. Each of these implementations has its own variables 
to configure. The implementation ``middle_name_to_initial`` is used for the 
``pre_processing`` step for ``ssa`` data to create a column that maches the ``w2`` 
``middle_initial`` column.

For all other pipeline steps, we've selected a default implementation, which 
either does nothing or simply passes inputs to outputs as appropriate.


For ``splink_blocking_and_filtering``, we set::

    LINK_ONLY: true
    BLOCKING_RULES: "'l.first_name == r.first_name,l.last_name == r.last_name'"

The first variable instructs Splink to link records between datasets without de-depulicating within 
datasets, respectively. 
The second is used by the Splink implementation to define which pairs of records 
will be considered as possible matches (records with matching first or last names).

For ``splink_evaluating_pairs``, we set::

  LINK_ONLY: true
  BLOCKING_RULES_FOR_TRAINING: "'l.first_name == r.first_name,l.last_name == r.last_name'"
  COMPARISONS: "'ssn:exact,first_name:exact,middle_initial:exact,last_name:exact'"
  PROBABILITY_TWO_RANDOM_RECORDS_MATCH: 0.0001  # == 1 / len(w2)

The first == two variables are used similarly to the previous implementation. The third 
defines the columns which will be compared by the Splink model, and how Splink will evaluate
whether the column values match (exact comparisons). The fourth is a parameter used in training
the model and making predictions (see the Splink docs for 
`more info <https://moj-analytical-services.github.io/splink/api_docs/training.html#splink.internals.linker_components.training.LinkerTraining.estimate_parameters_using_expectation_maximisation>`_). 

For ``splink_links_to_clusters``, as discussed earlier in the :ref:`implementation_configuration` section,
we set::

    THRESHOLD_MATCH_PROBABILITY: 0.996

And that's our naive Splink model! Next let's take a look at the results from when we ran the 
pipeline earlier.

Naive model results
===================

Input and output data is stored in Parquet files. For example, to see our original records, 
we can view the contents of the input files listed in ``input_data_demo.yaml`` using Python:

.. code-block:: console

  $ # Create/activate a conda environment if you don't want to install globally!
  $ pip install pandas pyarrow
  $ python
  >>> import pandas as pd
  >>> pd.read_parquet("2020/input_file_ssa.parquet")
        simulant_id          ssn first_name    middle_name  ...     sex event_type event_date Record ID
  0         0_19979  786-77-6454     Evelyn  Granddaughter  ...  Female   creation   19191204         0
  1          0_6846  688-88-6377     George         Robert  ...    Male   creation   19210616         1
  2         0_19983  651-33-9561   Beatrice         Jennie  ...  Female   creation   19220113         2
  3           0_262  665-25-7858       Eura         Nadine  ...  Female   creation   19220305         3
  4         0_12473  875-10-2359    Roberta           Ruth  ...  Female   creation   19220306         4
  ...           ...          ...        ...            ...  ...     ...        ...        ...       ...
  16492     0_20687  183-90-0619    Matthew        Michael  ...  Female   creation   20201229     16492
  16493     0_20686  803-81-8527     Jermey          Tyler  ...    Male   creation   20201229     16493
  16494     0_20692  170-62-5253  Brittanie         Lauren  ...  Female   creation   20201229     16494
  16495     0_20662  281-88-9330     Marcus         Jasper  ...    Male   creation   20201230     16495
  16496     0_20673  547-99-7034     Analia        Brielle  ...  Female   creation   20201231     16496
  [15984 rows x 10 columns]

  >>> pd.read_parquet("2020/input_file_w2.parquet")
      simulant_id household_id employer_id          ssn  ... mailing_address_zipcode tax_form tax_year Record ID
  0            0_4          0_8          95  584-16-0130  ...                   00000       W2     2020         0
  1            0_5          0_8          29  854-13-6295  ...                   00000       W2     2020         1
  2            0_5          0_8          30  854-13-6295  ...                   00000       W2     2020         2
  3         0_5621       0_2289          46  674-27-1745  ...                   00000       W2     2020         3
  4         0_5623       0_2289          83  794-23-1522  ...                   00000       W2     2020         4
  ...          ...          ...         ...          ...  ...                     ...      ...      ...       ...
  9898     0_18936       0_7621          23  006-92-7857  ...                   00000       W2     2020      9898
  9899     0_18936       0_7621          90  006-92-7857  ...                   00000       W2     2020      9899
  9900     0_18937       0_7621           1  182-82-5017  ...                   00000     1099     2020      9900
  9901     0_18937       0_7621         105  182-82-5017  ...                   00000     1099     2020      9901
  9902     0_18939       0_7621           9  283-97-5940  ...                   00000       W2     2020      9902
  [9903 rows x 25 columns]

  >>> pd.read_parquet("known_clusters.parquet")
  Empty DataFrame
  Columns: [Input Record Dataset, Input Record ID, Cluster ID]
  Index: []

It can also be useful to setup an alias to more easily preview parquet files. Add the following to your 
``.bash_aliases`` or ``.bashrc`` file, and restart your terminal.

.. code-block:: console

   pqprint() { python -c "import pandas as pd; print(pd.read_parquet('$1'))" ; }

Let's use the alias to print the results parquet, the location of which was printed when we ran the pipeline.

.. code-block:: console

  $ pqprint results/2025_06_26_10_13_31/result.parquet 
        Input Record Dataset  Input Record ID               Cluster ID
  0           input_file_ssa             4610   input_file_ssa-__-4610
  1           input_file_ssa             4612   input_file_ssa-__-4612
  2           input_file_ssa             4613   input_file_ssa-__-4613
  3           input_file_ssa             4614   input_file_ssa-__-4614
  4           input_file_ssa             4615   input_file_ssa-__-4615
  ...                    ...              ...                      ...
  25178        input_file_w2             4496  input_file_ssa-__-11207
  25179       input_file_ssa            14652  input_file_ssa-__-14652
  25180       input_file_ssa             9980  input_file_ssa-__-14652
  25181        input_file_w2             5349  input_file_ssa-__-14652
  25182        input_file_w2             5350  input_file_ssa-__-14652

  [25183 rows x 3 columns]

As we can see, the pipeline has successfully outputted a ``Cluster ID`` for every 
input record it was able to link to another record for our probability threshold 
of ``99.6%``. ``Cluster ID`` names are chosen by Splink based on the first record 
assigned to them.

.. note::

  Running the pipeline also generates a :download:`DAG.svg <DAG-naive-pipeline.svg>` file in 
  the results directory which shows the implementations, data dependencies and 
  input validations present in the pipeline. Due to the large number of steps, the figure is 
  not very readable when embedded in this page, but can be opened in a new tab to allow for
  zooming in.

To see how the model linked pairs of records before resolving them into clusters, we can 
look at the intermediate output produced by the ``splink_evaluating_pairs`` 
implementation::

  $ pqprint results/2025_06_26_10_13_31/intermediate/splink_evaluating_pairs/result.parquet 
        Left Record Dataset  Left Record ID Right Record Dataset  Right Record ID   Probability
  0           input_file_ssa           16314        input_file_w2             7604  5.593631e-06
  1           input_file_ssa           16318        input_file_w2             7604  5.593631e-06
  2           input_file_ssa           16326        input_file_w2             6049  5.593631e-06
  3           input_file_ssa           16351        input_file_w2             3549  5.593631e-06
  4           input_file_ssa           16353        input_file_w2             7434  5.593631e-06
  ...                    ...             ...                  ...              ...           ...
  515790      input_file_ssa            8586        input_file_w2              943  3.526073e-04
  515791      input_file_ssa            8591        input_file_w2             3326  7.227902e-07
  515792      input_file_ssa            8595        input_file_w2             3369  7.227902e-07
  515793      input_file_ssa            8596        input_file_w2             6458  3.526073e-04
  515794      input_file_ssa            8597        input_file_w2             3248  7.227902e-07

  [515795 rows x 5 columns]

The record pairs displayed in the preview are all far below the match threshold, but the full results could 
be investigated further using ``pandas.read_parquet()`` in a Python session.

The Splink implementations in our pipeline also produce some diagnostic charts which can be useful 
for evaluating results, such as the :download:`match weights chart <naive_match_weights.html>` 
(`Splink docs <https://moj-analytical-services.github.io/splink/charts/match_weights_chart.html>`_) and 
:download:`comparison viewer tool <naive_comparison_viewer.html>` 
(`Splink docs <https://moj-analytical-services.github.io/splink/charts/comparison_viewer_dashboard.html>`_). 
These charts are from the 
``diagnostics/splink_evaluating_pairs`` subdirectory of the results directory for each pipeline run.

Finally, since we are using simulated input datasets, and therefore know the ground truth of 
which records are truly links, we can directly see how our naive model performed with the help of 
a script to evaluate false positives and false negatives, :download:`print_fp_fn_w2_ssa.py`.
Download and run it::

  $ python print_fp_fn_w2_ssa.py results/2025_06_26_10_13_31 .996
  9292 true links
  For threshold 0.996, len(false_positives)=19; len(false_negatives)=188

In other words, with a threshold 
probability of 99.6%, out of 9,262 true links to be found, our model missed 19 (false negatives),
and additionally linked 188 pairs that shouldn't have been linked (false positives). 


Depending on our goals with the linked data, we might increase the threshold to reduce false positives,
at the cost of increased false negatives.
But this was a simple linkage model.
Let's improve it to see if we can get a better performance tradeoff!


Configuring an improved pipeline
================================
Next, let's modify our naive pipeline configuration YAML to try to improve our results. Primarily, we 
will change the ``COMPARISONS`` we pass to ``splink_evaluating_pairs`` to use flexible comparison 
methods rather than exact matches, allowing us to link records which have typos or other noise in them. We'll 
use a new pipeline configuration YAML, :download:`pipeline_demo_improved.yaml`, with these changes.

In ``splink_evaluating_pairs``, our implementation configuration will now look like this::

  LINK_ONLY: true
  BLOCKING_RULES_FOR_TRAINING: "'l.first_name == r.first_name,l.last_name == r.last_name'"
  COMPARISONS: "'ssn:levenshtein,first_name:name,middle_initial:exact,last_name:name'"
  PROBABILITY_TWO_RANDOM_RECORDS_MATCH: 0.0001  # == 1 / len(w2)

``COMPARISONS`` now uses 
`Levenshtein <https://moj-analytical-services.github.io/splink/api_docs/comparison_library.html#splink.comparison_library.LevenshteinAtThresholds>`_
comparisons for ``ssn``, and 
`Name <https://moj-analytical-services.github.io/splink/api_docs/comparison_library.html#splink.comparison_library.NameComparison>`_
comparisons for ``first_name`` and ``last_name``, to link similar but not identical SSNs and names.

By re-running the pipeline with these changes and then running the evauation script, we can see how our results compare::

  $ easylink run -p pipeline_demo_improved.yaml -i input_data_demo.yaml -e environment_local.yaml -I /mnt/team/simulation_science/priv/engineering/er_ecosystem/images
  $ python print_fp_fn_w2_ssa.py results/2025_06_26_11_08_57 .996
  9292 true links
  For threshold 0.996, len(false_positives)=19; len(false_negatives)=158

We eliminated 30 false negatives compared to the naive results, thanks to our model linking more records with columns that 
are similar but don't exactly match.

Linking 2030 datasets using improved pipeline
=============================================
Finally, let's run this same "improved" pipeline, but using :download:`input_data_demo_2030.yaml` 
as the input YAML, which uses the ``ssa`` and ``w2`` datasets from 2030 rather than 
2020. We can run the same pipeline on different data by changing only the input parameter::

  $ easylink run -p pipeline_demo_improved.yaml -i input_data_demo_2030.yaml -e environment_local.yaml -I /mnt/team/simulation_science/priv/engineering/er_ecosystem/images
  python print_fp_fn_w2_ssa.py results/2025_06_26_11_17_52 .996
  10345 true links
  For threshold 0.996, len(false_positives)=14; len(false_negatives)=149

Our results are similar with the 2030 data!
