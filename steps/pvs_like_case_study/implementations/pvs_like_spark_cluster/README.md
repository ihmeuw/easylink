# PVS-like case study

This directory contains a case study emulating the Census Bureau's Person Identification
Validation System.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the data generation notebook
3. Run the linking notebook

## Setup/install

**TODO: Make this README include the basic version in addition to the R and Spark versions.**

### R version

To exactly replicate the conda environment that was used when making the R version of
this case study, run

```
$ conda create -n pvs_like_case_study_r --file=pvs_like_case_study_r_lock.txt
$ conda activate pvs_like_case_study_r
$ Rscript -e "renv::restore(library=.libPaths())"
```

in this directory.

If you'd like to update conda packages, you can *approximately*
recreate the environment with:

```
$ conda env create -n pvs_like_case_study_r -f pvs_like_case_study_r_environment.yml
$ conda activate pvs_like_case_study
$ Rscript -e "renv::restore(library=.libPaths())"
```

Updating R packages should be done within the environment, installing them
and then calling `renv::snapshot(type = "all")`.

### Spark version

Unfortunately, it isn't possible to install Spark with conda.
Instead, I have used a Singularity image with Spark, and then activated
a conda environment inside it.

These instructions basically only work on the IHME cluster, because they assume that the
location of your conda, and the location where it creates new conda environments,
both are subdirectories of `/mnt`.
Also, the singularity pull assumes amd64 architecture.

```
$ conda create -n pvs_like_case_study_spark_local --file=pvs_like_case_study_spark_local_lock_no_jupyter.txt # or if you need jupyter, leave out the no_jupyter
$ singularity pull spark.sif docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63
```

If you'd like to update the conda packages and Singularity image:

```
$ conda env create -n pvs_like_case_study_spark_local -f pvs_like_case_study_spark_local_environment.yaml
$ singularity pull spark.sif docker://apache/spark:latest
# If you need Jupyter
$ conda activate pvs_like_case_study_spark_local
$ conda install jupyterlab
```

### (Optional) Spark nodes

You can use the Spark case study environment both to run the script, and to run Spark itself on
the master/worker nodes.
However, for the nodes, all you _need_ is Python, with the same version as you have in your
Spark case study environment.
You can create a separate conda environment for this.

## Run the linking notebook

### R version

Run the notebook `pvs_like_case_study_sample_data_r.ipynb`
in the `pvs_like_case_study_r` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ./convert_notebook.sh pvs_like_case_study_sample_data_r # only necessary if you've edited the notebook
$ python pvs_like_case_study_sample_data_r.py
```

### Local Spark version

```
$ mkdir /tmp/pvs_like_case_study_spark_$USER
# We don't use "singularity shell" because that runs a non-login shell, so conda wouldn't be on the PATH
$ singularity run -B /mnt:/mnt,/tmp/pvs_like_case_study_spark_$USER:/tmp spark.sif bash -l
Singularity> conda activate pvs_like_case_study_spark
(pvs_like_case_study_spark) Singularity> jupyter lab
```

or without Jupyter, replace the last line with `python pvs_like_case_study_sample_data_spark.py`.

### Distributed Spark version

First, start a Spark cluster. I do this by running `sbatch -A proj_simscience -p all.q start_spark_slurm.sh`
in this directory **outside of any srun (it will not work otherwise)**.
You'll need to edit the CONDA_PATH variable in that script to point to the conda you used to create the
environment described above.
You should edit CONDA_ENV to either the name of your Spark case study environment, or a minimal
environment for the nodes as described above.

Look at the Slurm logs of that script to find the Spark master URL and copy it. Then:

```
$ mkdir /tmp/pvs_like_case_study_spark_$USER
# We don't use "singularity shell" because that runs a non-login shell, so conda wouldn't be on the PATH
$ singularity run -B /mnt:/mnt,/tmp/pvs_like_case_study_spark_$USER:/tmp spark.sif bash -l
Singularity> export LINKER_SPARK_MASTER_URL=<your Spark master URL>
Singularity> conda activate pvs_like_case_study_spark
(pvs_like_case_study_spark) Singularity> jupyter lab
```

or without Jupyter, replace the last line with `python pvs_like_case_study_sample_data_spark.py`.
