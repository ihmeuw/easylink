# PVS-like case study

This directory contains a case study emulating the Census Bureau's Person Identification
Validation System.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the data generation notebook
3. Run the linking notebook

## Create the appropriate conda environment

**TODO: Make this README include both the R version and the non-R version.**

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

## Run the linking notebook

Run the notebook `pvs_like_case_study_sample_data_r.ipynb`
in the `pvs_like_case_study_r` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ./convert_notebook.sh pvs_like_case_study_sample_data_r # only necessary if you've edited the notebook
$ python pvs_like_case_study_sample_data_r.py
```