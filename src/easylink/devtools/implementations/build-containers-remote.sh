#!/bin/bash

set -e

cd python_pandas
singularity build --remote --force python_pandas.sif python_pandas.def

cd ../python_pyspark
singularity build --remote --force python_pyspark.sif python_pyspark.def

cd ../r
singularity build --remote --force r-image.sif r-image.def