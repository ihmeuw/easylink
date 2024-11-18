#!/bin/bash

set -e

cd python_pandas
sudo singularity build --force python-pandas-image.sif python-pandas.def

cd ../python_pyspark
sudo singularity build --force python-pyspark-image.sif python-pyspark.def

cd ../r
sudo singularity build --force r-image.sif r-image.def