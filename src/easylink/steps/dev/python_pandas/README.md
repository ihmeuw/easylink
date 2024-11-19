# Pandas container performing the dummy step

## Running with Singularity

### With Root Access

If you have root access on your machine, you can directly build the container from the Singularity .def file:

```
sudo singularity build --force python_pandas.sif python_pandas.def
mkdir -p /tmp/dummy_step_python_pandas_results/
singularity run --pwd / -B ../input_data/:/input_data/,/tmp/dummy_step_python_pandas_results/:/results ./python_pandas.sif
```

### Without Root Access

If you are on a system (e.g. HPC cluster) without root access, you can still build the container remotely. 
This requires a (free, but throttled) account at https://cloud.sylabs.io/ to build containers in the cloud through
Singularity Container Services.

```
singularity build --remote python_pandas.sif python_pandas.def
mkdir -p /tmp/dummy_step_python_pandas_results/
singularity run --pwd / -B ../input_data/:/input_data/,/tmp/dummy_step_python_pandas_results/:/results ./python_pandas.sif
```