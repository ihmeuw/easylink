.. _getting_started:

===============
Getting Started
===============

Installation
============

See `github <https://github.com/ihmeuw/easylink>`_

First Pipeline
==============

`common/pipeline.yaml`
----------------------
Let's run our first pipeline, by passing the pipeline specification, input data specification, and 
environment specification files to the easylink run command. 
This will validate the pipeline specification against the pipeline schema and configure and run the pipeline.

.. code-block:: console

   $ conda activate easylink
   $ cd tests
   $ easylink run -p specifications/common/pipeline.yaml -i specifications/common/input_data.yaml -e specifications/e2e/environment_slurm.yaml
   2025-04-01 06:51:22.146 | 0:00:09.139203 | run:158 - Running pipeline
   2025-04-01 06:51:22.147 | 0:00:09.139873 | run:160 - Results directory: /mnt/share/homes/tylerdy/easylink/tests/results/2025_04_01_06_51_22
   2025-04-01 06:51:26.220 | 0:00:13.212825 | main:115 - Running Snakemake
   [Tue Apr  1 06:51:26 2025]
   Job 9: Validating step_4_python_pandas input slot step_4_secondary_input
   Reason: Missing output files: input_validations/step_4_python_pandas/step_4_secondary_input_validator
   [Tue Apr  1 06:51:26 2025]
   Job 6: Validating step_1_python_pandas input slot step_1_main_input
   Reason: Missing output files: input_validations/step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 06:51:27 2025]
   Job 5: Running step_1 implementation: step_1_python_pandas
   Reason: Missing output files: intermediate/step_1_python_pandas/result.parquet; Input files updated by another job: input_validations/step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:23:28 2025]
   Job 7: Validating step_2_python_pandas input slot step_2_main_input
   Reason: Missing output files: input_validations/step_2_python_pandas/step_2_main_input_validator; Input files updated by another job: intermediate/step_1_python_pandas/result.parquet
   [Tue Apr  1 07:23:28 2025]
   Job 4: Running step_2 implementation: step_2_python_pandas
   Reason: Missing output files: intermediate/step_2_python_pandas/result.parquet; Input files updated by another job: input_validations/step_2_python_pandas/step_2_main_input_validator, intermediate/step_1_python_pandas/result.parquet
   [Tue Apr  1 07:23:58 2025]
   Job 8: Validating step_3_python_pandas input slot step_3_main_input
   Reason: Missing output files: input_validations/step_3_python_pandas/step_3_main_input_validator; Input files updated by another job: intermediate/step_2_python_pandas/result.parquet
   [Tue Apr  1 07:23:58 2025]
   Job 3: Splitting step_3_python_pandas step_3_main_input into chunks
   Reason: Missing output files: <TBD>; Input files updated by another job: input_validations/step_3_python_pandas/step_3_main_input_validator, intermediate/step_2_python_pandas/result.parquet
   DAG of jobs will be updated after completion.
   2025-04-01 07:23:59.058 | 0:32:46.050511 | split_data_by_size:54 - Input data is already smaller than desired chunk size; not splitting
   [Tue Apr  1 07:23:59 2025]
   Job 14: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_python_pandas/processed/chunk_0/result.parquet
   [Tue Apr  1 07:25:51 2025]
   Job 2: Aggregating step_3_python_pandas step_3_main_output
   Reason: Missing output files: intermediate/step_3_python_pandas/result.parquet; Input files updated by another job: intermediate/step_3_python_pandas/processed/chunk_0/result.parquet
   2025-04-01 07:25:51.193 | 0:34:38.186010 | concatenate_datasets:28 - Concatenating 1 datasets
   [Tue Apr  1 07:25:51 2025]
   Job 10: Validating step_4_python_pandas input slot step_4_main_input
   Reason: Missing output files: input_validations/step_4_python_pandas/step_4_main_input_validator; Input files updated by another job: intermediate/step_3_python_pandas/result.parquet
   [Tue Apr  1 07:25:51 2025]
   Job 1: Running step_4 implementation: step_4_python_pandas
   Reason: Missing output files: intermediate/step_4_python_pandas/result.parquet; Input files updated by another job: intermediate/step_3_python_pandas/result.parquet, input_validations/step_4_python_pandas/step_4_main_input_validator, input_validations/step_4_python_pandas/step_4_secondary_input_validator
   [Tue Apr  1 07:26:18 2025]
   Job 11: Validating results input slot main_input
   Reason: Missing output files: input_validations/final_validator; Input files updated by another job: intermediate/step_4_python_pandas/result.parquet
   [Tue Apr  1 07:26:18 2025]
   Job 0: Grabbing final output
   Reason: Missing output files: result.parquet; Input files updated by another job: intermediate/step_4_python_pandas/result.parquet, input_validations/final_validator

Inputs and outputs
------------------
Input and output data is stored in parquet files. The locations of the input data files passed to easylink in our last command is specifications/common/input_data.yaml.
We can check what is in these files using code like the example below.

.. code-block:: console

   $ python
   >>> import pandas as pd
   >>> print(pd.read_parquet("/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"))
          foo bar  counter
   0        0   a        0
   1        1   b        0
   2        2   c        0
   3        3   d        0
   4        4   e        0
   ...    ...  ..      ...
   9995  9995   a        0
   9996  9996   b        0
   9997  9997   c        0
   9998  9998   d        0
   9999  9999   e        0

   [10000 rows x 3 columns]

The other two input files look identical, each with 10000 rows.

It can also be useful to setup an alias to more easily preview parquet files. Add the following to your 
.bash_aliases or .bashrc file, and restart your terminal.

.. code-block:: console

   pqprint() { python -c "import pandas as pd; print(pd.read_parquet('$1'))" ; }

Let's use the alias to print the results parquet, the location of which was printed when we ran the pipeline.

.. code-block:: console

   $ pqprint /mnt/share/homes/tylerdy/easylink/tests/results/2025_04_01_06_51_22
           foo bar  counter  added_column_0  added_column_1  added_column_2  added_column_3  added_column_4
   0         0   a        4             0.0             1.0             2.0             3.0               4
   1         1   b        4             0.0             1.0             2.0             3.0               4
   2         2   c        4             0.0             1.0             2.0             3.0               4
   3         3   d        4             0.0             1.0             2.0             3.0               4
   4         4   e        4             0.0             1.0             2.0             3.0               4
   ...     ...  ..      ...             ...             ...             ...             ...             ...
   59995  9995   a        1             0.0             0.0             0.0             0.0               4
   59996  9996   b        1             0.0             0.0             0.0             0.0               4
   59997  9997   c        1             0.0             0.0             0.0             0.0               4
   59998  9998   d        1             0.0             0.0             0.0             0.0               4
   59999  9999   e        1             0.0             0.0             0.0             0.0               4

If we compare the input data to the results, we can see that new columns were added, the data now has 60k rows, 
the counter column is incremented for many rows, and other columns have different values for different rows 
as well.
Next we will examine the steps the pipeline executed, where they are defined/implemented, and how they transformed 
the data.

Pipeline schema and steps
-------------------------
The pipeline specification we passed to ``easylink run``, `specifications/common/pipeline.yaml`, 
configures the pipeline schema for this run, by specifying configuration details for each step 
defined by the schema. The schema steps, and the edges between them, are defined in 
`pipeline_schema_constants/development.py`. The schema steps, or nodes, define input and outputs slots for 
data used or produced by the schema steps, as well as any logical or behavioral structure of the step,
such as defining a step as a ``LoopStep``, ``ParallelStep``, ``ChoiceStep``, or ``HierarchicalStep``. The edges 
define how data moves between steps' input and output slots.

`pipeline_schema_constants/development.py` defines that the pipeline schema requires four steps, that the 
third step is ``EmbarrassinglyParallel``, that the fourth step is a ``ChoiceStep``, and that all steps have 
one input except the fourth step, which has two.

An implementation is chosen for each step, which defines a 
container, script, outputs and other details for a step. The implementations for steps of this pipeline are 
defined in `implementation_metadata.yaml`.

In this file you can see that `step_1_python_pandas`, `step_2_python_pandas` and `step_3_python_pandas` 
have no value for `INPUT_ENV_VARS`, but `step_4_python_pandas` does. `INPUT_ENV_VARS` has a default value of 
`DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS`, and `step_4_python_pandas` adds 
`DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS`. The edges in `pipeline_schema_constants/development.py` connect
these inputs to step outputs.

.. todo:: 
   Where are these env vars being set to actual paths? What happens after input_data_config
   in `PipelineSchema.configure_pipeline`? Where is step 4 secondary input coming from?
   Why is there one input coming directly from the previous step, and one from validation step?

Running the pipeline generates a DAG.svg file in the results directory which shows the steps and edges of the 
pipeline schema as it is configured.

.. image:: DAG-common-pipeline.svg
   :width: 400

As you can see, each step has a single input (well, it doesnt really look like this) and output, 
except `step_4` has two inputs, as defined in 
`pipeline_schema_constants/development.py`. 

Now we can understand why the final output has 60k rows. When there are multiple input data files, the rows 
in the files are concatenated. So `step_1` concatenates three 10k row datasets, and `step_4` concatenates these 
30k rows with another 30k rows.

`step_3` is aggregated and split because it is defined as 
`EmbarrassinglyParallel`.

We've already viewed the final output, but if we want to see how the data is transformed over the course 
of the pipeline, we can view intermediary outputs as well::

   $ pqprint /ihme/homes/tylerdy/easylink/tests/results/2025_04_01_06_51_22/intermediate/step_1_python_pandas/result.parquet
            foo bar  counter  added_column_0  added_column_1
   0         0   a        1               0               1
   1         1   b        1               0               1
   2         2   c        1               0               1
   3         3   d        1               0               1
   4         4   e        1               0               1
   ...     ...  ..      ...             ...             ...
   29995  9995   a        1               0               1
   29996  9996   b        1               0               1
   29997  9997   c        1               0               1
   29998  9998   d        1               0               1
   29999  9999   e        1               0               1

   [30000 rows x 5 columns]

.. todo::
   * Explain Out of order job messages - snakemake jobs

More Pipeline Specifications
============================
The easylink tests folder includes several other pipeline specification files (yaml). While some are special 
configurations utilized by the testing infrastructure, others can be run directly using the command line - the 
ones with four steps. Let's try running another complete pipeline.

`e2e/pipeline.yaml`
-------------------

.. code-block:: console

   $ easylink run -p specifications/e2e/pipeline.yaml -i specifications/common/input_data.yaml -e specifications/e2e/environment_slurm.yaml
   2025-04-02 09:37:40.320 | 0:00:01.436867 | run:158 - Running pipeline
   2025-04-02 09:37:40.321 | 0:00:01.437074 | run:160 - Results directory: /mnt/share/homes/tylerdy/easylink/tests/results/2025_04_02_09_37_40
   2025-04-02 09:37:43.689 | 0:00:04.804912 | main:115 - Running Snakemake
   [Wed Apr  2 09:37:44 2025]
   localrule wait_for_spark_master:
      output: spark_logs/spark_master_uri.txt
      jobid: 9
      reason: Missing output files: spark_logs/spark_master_uri.txt
      resources: mem_mb=1024, mem_mib=977, disk_mb=1000, disk_mib=954, tmpdir=/tmp, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1
   [Wed Apr  2 09:37:44 2025]
   Job 6: Validating step_1_python_pandas input slot step_1_main_input
   Reason: Missing output files: input_validations/step_1_python_pandas/step_1_main_input_validator
   [Wed Apr  2 09:37:44 2025]
   Job 11: Validating step_4_r input slot step_4_secondary_input
   Reason: Missing output files: input_validations/step_4_r/step_4_secondary_input_validator
   [Wed Apr  2 09:37:44 2025]
   rule start_spark_master:
      output: spark_logs/spark_master_log.txt
      jobid: 15
      reason: Missing output files: spark_logs/spark_master_log.txt
      resources: mem_mb=1524, mem_mib=1454, disk_mb=1000, disk_mib=954, tmpdir=<TBD>, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1, slurm_extra=--output 'spark_logs/start_spark_master-slurm-%j.log'
   Searching for Spark master URL in spark_logs/spark_master_log.txt
   [Wed Apr  2 09:37:44 2025]
   Job 5: Running step_1 implementation: step_1_python_pandas
   Reason: Missing output files: intermediate/step_1_python_pandas/result.parquet; Input files updated by another job: input_validations/step_1_python_pandas/step_1_main_input_validator
   Unable to find Spark master URL in logfile. Waiting 10 seconds and retrying...
   (attempt 1/20)
   Spark master URL found: spark://gen-slurm-sarchive-p0008.cluster.ihme.washington.edu:28508
   [Wed Apr  2 09:38:04 2025]
   localrule wait_for_spark_worker:
      input: spark_logs/spark_master_uri.txt
      output: spark_logs/spark_worker_started_1-of-1.txt
      jobid: 8
      reason: Missing output files: spark_logs/spark_worker_started_1-of-1.txt; Input files updated by another job: spark_logs/spark_master_uri.txt
      wildcards: scatteritem=1-of-1
      resources: mem_mb=1024, mem_mib=977, disk_mb=1000, disk_mib=954, tmpdir=/tmp, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1
   [Wed Apr  2 09:38:04 2025]
   localrule split_workers:
      input: spark_logs/spark_master_uri.txt
      output: spark_logs/spark_worker_1-of-1.txt
      jobid: 17
      reason: Missing output files: spark_logs/spark_worker_1-of-1.txt; Input files updated by another job: spark_logs/spark_master_uri.txt
      resources: mem_mb=1024, mem_mib=977, disk_mb=1000, disk_mib=954, tmpdir=/tmp, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1
   Waiting for Spark Worker 1-of-1 to start...
   [Wed Apr  2 09:38:04 2025]
   rule start_spark_worker:
      input: spark_logs/spark_master_uri.txt, spark_logs/spark_worker_1-of-1.txt
      output: spark_logs/spark_worker_log_1-of-1.txt
      jobid: 16
      reason: Missing output files: spark_logs/spark_worker_log_1-of-1.txt; Input files updated by another job: spark_logs/spark_worker_1-of-1.txt, spark_logs/spark_master_uri.txt
      wildcards: scatteritem=1-of-1
      resources: mem_mb=1524, mem_mib=1454, disk_mb=1000, disk_mib=954, tmpdir=<TBD>, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1, slurm_extra=--output 'spark_logs/start_spark_worker-slurm-%j.log'
   [Wed Apr  2 09:38:24 2025]
   Job 7: Validating step_2_python_pyspark input slot step_2_main_input
   Reason: Missing output files: input_validations/step_2_python_pyspark/step_2_main_input_validator; Input files updated by another job: intermediate/step_1_python_pandas/result.parquet
   Unable to find Spark worker 1-of-1 registration. Waiting 20 seconds and retrying...
   (attempt 1/20)
   Unable to find Spark worker 1-of-1 registration. Waiting 20 seconds and retrying...
   (attempt 2/20)
   Spark Worker 1-of-1 registered successfully
   [Wed Apr  2 09:39:04 2025]
   Job 4: Running step_2 implementation: step_2_python_pyspark
   Reason: Missing output files: intermediate/step_2_python_pyspark/result.parquet; Input files updated by another job: intermediate/step_1_python_pandas/result.parquet, input_validations/step_2_python_pyspark/step_2_main_input_validator, spark_logs/spark_master_uri.txt, spark_logs/spark_worker_started_1-of-1.txt
   [Wed Apr  2 09:40:04 2025]
   Job 10: Validating step_3_python_pandas input slot step_3_main_input
   Reason: Missing output files: input_validations/step_3_python_pandas/step_3_main_input_validator; Input files updated by another job: intermediate/step_2_python_pyspark/result.parquet
   [Wed Apr  2 09:40:04 2025]
   Job 3: Splitting step_3_python_pandas step_3_main_input into chunks
   Reason: Missing output files: <TBD>; Input files updated by another job: intermediate/step_2_python_pyspark/result.parquet, input_validations/step_3_python_pandas/step_3_main_input_validator
   DAG of jobs will be updated after completion.
   2025-04-02 09:40:04.932 | 0:02:26.048512 | split_data_by_size:55 - Input data is already smaller than desired chunk size; not splitting
   [Wed Apr  2 09:40:05 2025]
   Job 20: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_python_pandas/processed/chunk_0/result.parquet
   [Wed Apr  2 09:40:34 2025]
   Job 2: Aggregating step_3_python_pandas step_3_main_output
   Reason: Missing output files: intermediate/step_3_python_pandas/result.parquet; Input files updated by another job: intermediate/step_3_python_pandas/processed/chunk_0/result.parquet
   2025-04-02 09:40:34.897 | 0:02:56.013744 | concatenate_datasets:29 - Concatenating 1 datasets
   [Wed Apr  2 09:40:34 2025]
   Job 12: Validating step_4_r input slot step_4_main_input
   Reason: Missing output files: input_validations/step_4_r/step_4_main_input_validator; Input files updated by another job: intermediate/step_3_python_pandas/result.parquet
   [Wed Apr  2 09:40:35 2025]
   Job 1: Running step_4 implementation: step_4_r
   Reason: Missing output files: intermediate/step_4_r/result.parquet; Input files updated by another job: input_validations/step_4_r/step_4_secondary_input_validator, intermediate/step_3_python_pandas/result.parquet, input_validations/step_4_r/step_4_main_input_validator
   [Wed Apr  2 09:41:04 2025]
   localrule terminate_spark:
      input: intermediate/step_4_r/result.parquet
      output: spark_logs/spark_master_terminated.txt
      jobid: 14
      reason: Missing output files: spark_logs/spark_master_terminated.txt; Input files updated by another job: intermediate/step_4_r/result.parquet
      resources: mem_mb=1024, mem_mib=977, disk_mb=1000, disk_mib=954, tmpdir=/tmp, slurm_account=proj_simscience, slurm_partition=all.q, runtime=60, cpus_per_task=1
   [Wed Apr  2 09:41:04 2025]
   Job 13: Validating results input slot main_input
   Reason: Missing output files: input_validations/final_validator; Input files updated by another job: intermediate/step_4_r/result.parquet
   [Wed Apr  2 09:42:05 2025]
   Job 0: Grabbing final output
   Reason: Missing output files: result.parquet; Input files updated by another job: intermediate/step_4_r/result.parquet, input_validations/final_validator, spark_logs/spark_master_log.txt, spark_logs/spark_worker_log_1-of-1.txt, spark_logs/spark_master_terminated.txt


.. code-block:: console

   $ pqprint /ihme/homes/tylerdy/easylink/tests/results/2025_04_02_09_37_40/result.parquet
         foo bar  counter  ...  added_column_1713  added_column_1714  added_column_1715
   0         0   a     1715  ...               1713               1714               1715
   1         1   b     1715  ...               1713               1714               1715
   2         2   c     1715  ...               1713               1714               1715
   3         3   d     1715  ...               1713               1714               1715
   4         4   e     1715  ...               1713               1714               1715
   ...     ...  ..      ...  ...                ...                ...                ...
   59995  9995   a      912  ...               1713               1714               1715
   59996  9996   b      912  ...               1713               1714               1715
   59997  9997   c      912  ...               1713               1714               1715
   59998  9998   d      912  ...               1713               1714               1715
   59999  9999   e      912  ...               1713               1714               1715

   [60000 rows x 8 columns]

.. image:: DAG-e2e-pipeline.svg
   :width: 500


`e2e/pipeline_expanded.yaml`
----------------------------

.. code-block:: console

   $ easylink run -p specifications/e2e/pipeline_expanded.yaml -i specifications/common/input_data.yaml -e specifications/e2e/environment_slurm.yaml
   2025-04-01 07:04:16.812 | 0:00:01.500753 | run:158 - Running pipeline
   2025-04-01 07:04:16.812 | 0:00:01.500984 | run:160 - Results directory: /mnt/share/homes/tylerdy/easylink/tests/results/2025_04_01_07_04_16
   2025-04-01 07:04:19.300 | 0:00:03.989113 | main:115 - Running Snakemake
   [Tue Apr  1 07:04:20 2025]
   Job 19: Validating step_4b_python_pandas input slot step_4b_secondary_input
   Reason: Missing output files: input_validations/step_4b_python_pandas/step_4b_secondary_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 11: Validating step_1_parallel_split_2_step_1_python_pandas input slot step_1_main_input
   Reason: Missing output files: input_validations/step_1_parallel_split_2_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 17: Validating step_4a_python_pandas input slot step_4a_secondary_input
   Reason: Missing output files: input_validations/step_4a_python_pandas/step_4a_secondary_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 9: Validating step_1_parallel_split_1_step_1_python_pandas input slot step_1_main_input
   Reason: Missing output files: input_validations/step_1_parallel_split_1_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 13: Validating step_1_parallel_split_3_step_1_python_pandas input slot step_1_main_input
   Reason: Missing output files: input_validations/step_1_parallel_split_3_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 10: Running step_1 implementation: step_1_python_pandas
   Reason: Missing output files: intermediate/step_1_parallel_split_2_step_1_python_pandas/result.parquet; Input files updated by another job: input_validations/step_1_parallel_split_2_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 12: Running step_1 implementation: step_1_python_pandas
   Reason: Missing output files: intermediate/step_1_parallel_split_3_step_1_python_pandas/result.parquet; Input files updated by another job: input_validations/step_1_parallel_split_3_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:04:20 2025]
   Job 8: Running step_1 implementation: step_1_python_pandas
   Reason: Missing output files: intermediate/step_1_parallel_split_1_step_1_python_pandas/result.parquet; Input files updated by another job: input_validations/step_1_parallel_split_1_step_1_python_pandas/step_1_main_input_validator
   [Tue Apr  1 07:22:21 2025]
   Job 14: Validating step_2_python_pandas input slot step_2_main_input
   Reason: Missing output files: input_validations/step_2_python_pandas/step_2_main_input_validator; Input files updated by another job: intermediate/step_1_parallel_split_3_step_1_python_pandas/result.parquet, intermediate/step_1_parallel_split_2_step_1_python_pandas/result.parquet, intermediate/step_1_parallel_split_1_step_1_python_pandas/result.parquet
   [Tue Apr  1 07:22:21 2025]
   Job 7: Running step_2 implementation: step_2_python_pandas
   Reason: Missing output files: intermediate/step_2_python_pandas/result.parquet; Input files updated by another job: intermediate/step_1_parallel_split_3_step_1_python_pandas/result.parquet, input_validations/step_2_python_pandas/step_2_main_input_validator, intermediate/step_1_parallel_split_2_step_1_python_pandas/result.parquet, intermediate/step_1_parallel_split_1_step_1_python_pandas/result.parquet
   [Tue Apr  1 07:23:21 2025]
   Job 15: Validating step_3_loop_1_step_3_python_pandas input slot step_3_main_input
   Reason: Missing output files: input_validations/step_3_loop_1_step_3_python_pandas/step_3_main_input_validator; Input files updated by another job: intermediate/step_2_python_pandas/result.parquet
   [Tue Apr  1 07:23:21 2025]
   Job 6: Splitting step_3_loop_1_step_3_python_pandas step_3_main_input into chunks
   Reason: Missing output files: <TBD>; Input files updated by another job: input_validations/step_3_loop_1_step_3_python_pandas/step_3_main_input_validator, intermediate/step_2_python_pandas/result.parquet
   DAG of jobs will be updated after completion.
   2025-04-01 07:23:21.766 | 0:19:06.455365 | split_data_by_size:56 - Splitting a 0.17 MB dataset (90000 rows) into into 2 chunks of size ~0.1 MB each
   [Tue Apr  1 07:23:21 2025]
   Job 25: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_loop_1_step_3_python_pandas/processed/chunk_0/result.parquet
   [Tue Apr  1 07:23:22 2025]
   Job 26: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_loop_1_step_3_python_pandas/processed/chunk_1/result.parquet
   [Tue Apr  1 07:24:21 2025]
   Job 5: Aggregating step_3_loop_1_step_3_python_pandas step_3_main_output
   Reason: Missing output files: intermediate/step_3_loop_1_step_3_python_pandas/result.parquet; Input files updated by another job: intermediate/step_3_loop_1_step_3_python_pandas/processed/chunk_1/result.parquet, intermediate/step_3_loop_1_step_3_python_pandas/processed/chunk_0/result.parquet
   2025-04-01 07:24:21.609 | 0:20:06.298319 | concatenate_datasets:28 - Concatenating 2 datasets
   [Tue Apr  1 07:24:21 2025]
   Job 16: Validating step_3_loop_2_step_3_python_pandas input slot step_3_main_input
   Reason: Missing output files: input_validations/step_3_loop_2_step_3_python_pandas/step_3_main_input_validator; Input files updated by another job: intermediate/step_3_loop_1_step_3_python_pandas/result.parquet
   [Tue Apr  1 07:24:21 2025]
   Job 4: Splitting step_3_loop_2_step_3_python_pandas step_3_main_input into chunks
   Reason: Missing output files: <TBD>; Input files updated by another job: input_validations/step_3_loop_2_step_3_python_pandas/step_3_main_input_validator, intermediate/step_3_loop_1_step_3_python_pandas/result.parquet
   DAG of jobs will be updated after completion.
   2025-04-01 07:24:21.787 | 0:20:06.476146 | split_data_by_size:56 - Splitting a 0.17 MB dataset (90000 rows) into into 2 chunks of size ~0.1 MB each
   [Tue Apr  1 07:24:21 2025]
   Job 30: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_loop_2_step_3_python_pandas/processed/chunk_1/result.parquet
   [Tue Apr  1 07:24:22 2025]
   Job 29: Running step_3 implementation: step_3_python_pandas
   Reason: Missing output files: intermediate/step_3_loop_2_step_3_python_pandas/processed/chunk_0/result.parquet
   [Tue Apr  1 07:25:21 2025]
   Job 3: Aggregating step_3_loop_2_step_3_python_pandas step_3_main_output
   Reason: Missing output files: intermediate/step_3_loop_2_step_3_python_pandas/result.parquet; Input files updated by another job: intermediate/step_3_loop_2_step_3_python_pandas/processed/chunk_1/result.parquet, intermediate/step_3_loop_2_step_3_python_pandas/processed/chunk_0/result.parquet
   2025-04-01 07:25:21.785 | 0:21:06.474036 | concatenate_datasets:28 - Concatenating 2 datasets
   [Tue Apr  1 07:25:21 2025]
   Job 18: Validating step_4a_python_pandas input slot step_4a_main_input
   Reason: Missing output files: input_validations/step_4a_python_pandas/step_4a_main_input_validator; Input files updated by another job: intermediate/step_3_loop_2_step_3_python_pandas/result.parquet
   [Tue Apr  1 07:25:21 2025]
   Job 2: Running step_4a implementation: step_4a_python_pandas
   Reason: Missing output files: intermediate/step_4a_python_pandas/result.parquet; Input files updated by another job: input_validations/step_4a_python_pandas/step_4a_secondary_input_validator, intermediate/step_3_loop_2_step_3_python_pandas/result.parquet, input_validations/step_4a_python_pandas/step_4a_main_input_validator
   [Tue Apr  1 07:26:21 2025]
   Job 20: Validating step_4b_python_pandas input slot step_4b_main_input
   Reason: Missing output files: input_validations/step_4b_python_pandas/step_4b_main_input_validator; Input files updated by another job: intermediate/step_4a_python_pandas/result.parquet
   [Tue Apr  1 07:26:22 2025]
   Job 1: Running step_4b implementation: step_4b_python_pandas
   Reason: Missing output files: intermediate/step_4b_python_pandas/result.parquet; Input files updated by another job: intermediate/step_4a_python_pandas/result.parquet, input_validations/step_4b_python_pandas/step_4b_main_input_validator, input_validations/step_4b_python_pandas/step_4b_secondary_input_validator
   [Tue Apr  1 07:27:22 2025]
   Job 21: Validating results input slot main_input
   Reason: Missing output files: input_validations/final_validator; Input files updated by another job: intermediate/step_4b_python_pandas/result.parquet
   [Tue Apr  1 07:27:22 2025]
   Job 0: Grabbing final output
   Reason: Missing output files: result.parquet; Input files updated by another job: intermediate/step_4b_python_pandas/result.parquet, input_validations/final_validator


.. code-block:: console

   $ pqprint /ihme/homes/tylerdy/easylink/tests/results/2025_04_01_07_04_16/result.parquet
            foo bar  counter  added_column_2  added_column_3  added_column_4  added_column_5  added_column_6
   0          0   a        6             2.0             3.0             4.0             5.0               6
   1          1   b        6             2.0             3.0             4.0             5.0               6
   2          2   c        6             2.0             3.0             4.0             5.0               6
   3          3   d        6             2.0             3.0             4.0             5.0               6
   4          4   e        6             2.0             3.0             4.0             5.0               6
   ...      ...  ..      ...             ...             ...             ...             ...             ...
   149995  9995   a        1             0.0             0.0             0.0             0.0               6
   149996  9996   b        1             0.0             0.0             0.0             0.0               6
   149997  9997   c        1             0.0             0.0             0.0             0.0               6
   149998  9998   d        1             0.0             0.0             0.0             0.0               6
   149999  9999   e        1             0.0             0.0             0.0             0.0               6

   [150000 rows x 8 columns]

.. image:: DAG-e2e-pipeline-expanded.svg
   :width: 600


That's all the valid pipelines currently available in the easylink `tests` directory! Next we will create
some pipelines of our own to run by copying the `tests` pipelines and making some changes.