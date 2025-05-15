# PySpark container performing the dummy step

Similar to Pandas, except it requires a writeable working directory, which means the Singularity instructions are a bit different.

This container has an additional required configurable environment variable, `DUMMY_STEP_SPARK_MASTER_URL`, which is the URL of the Spark
master.

## Running with Singularity

### With Root Access

If you have root access on your machine, you can directly build the container from the Singularity .def file:

```
sudo singularity build --force python_pyspark.sif python_pyspark.def
mkdir -p /tmp/dummy_step_python_pyspark_results/
mkdir -p /tmp/dummy_step_python_pyspark_workdir/
singularity run --pwd /workdir -B ../input_data/input_file_1.parquet:/input_data/main_input_file.parquet,/tmp/dummy_step_python_pyspark_results/:/results,/tmp/dummy_step_python_pyspark_workdir/:/workdir ./python_pyspark.sif
```

### Without Root Access

If you are on a system (e.g. HPC cluster) without root access, you can still build the container remotely. 
This requires a (free, but throttled) account at https://cloud.sylabs.io/ to build containers in the cloud through
Singularity Container Services.

```
singularity build --remote python_pyspark.sif python_pyspark.def
mkdir -p /tmp/dummy_step_python_pyspark_results/
mkdir -p /tmp/dummy_step_python_pyspark_workdir/
singularity run --pwd /workdir -B ../input_data/input_file_1.parquet:/input_data/main_input_file.parquet,/tmp/dummy_step_python_pyspark_results/:/results,/tmp/dummy_step_python_pyspark_workdir/:/workdir ./python_pyspark.sif
```