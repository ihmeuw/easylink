# R container performing the dummy step

Similar to Pandas, except it requires a writeable /tmp directory, which means the Singularity instructions are a bit different.

## Running with Singularity

If you have root access on your machine, you can directly build the container from the Singularity .def file:

```
sudo singularity build --force r-image.sif r-image.def
mkdir -p /tmp/dummy_step_r_results/
mkdir -p /tmp/dummy_step_r_tmp/
singularity run --pwd / -B ../input_data/input_file_1.parquet:/input_data/main_input_file.parquet,/tmp/dummy_step_r_results/:/results,/tmp/dummy_step_r_tmp/:/tmp ./r-image.sif
```

### Without Root Access

If you are on a system (e.g. HPC cluster) without root access, you can still build the container remotely. 
This requires a (free, but throttled) account at https://cloud.sylabs.io/ to build containers in the cloud through
Singularity Container Services.

```
singularity build --remote r-image.sif r-image.def
mkdir -p /tmp/dummy_step_r_results/
mkdir -p /tmp/dummy_step_r_tmp/
singularity run --pwd / -B ../input_data/input_file_1.parquet:/input_data/main_input_file.parquet,/tmp/dummy_step_r_results/:/results,/tmp/dummy_step_r_tmp/:/tmp ./r-image.sif
```