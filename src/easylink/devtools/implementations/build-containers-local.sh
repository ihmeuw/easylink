#!/bin/bash

set -e

cd python_pandas
sudo singularity build --force python_pandas.sif python_pandas.def

cd ../python_pyspark
sudo singularity build --force python_pyspark.sif python_pyspark.def

cd ../r
sudo singularity build --force r-image.sif r-image.def