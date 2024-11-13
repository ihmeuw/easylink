# Pandas container performing the dummy step

## Running with Singularity

Install [`spython` from PyPI](https://github.com/singularityhub/singularity-cli). Then:

```
spython recipe Dockerfile Singularity
singularity build --force python-pandas-image.sif Singularity
mkdir -p /tmp/dummy_step_python_pandas_results/
singularity run --pwd / -B ../input_file_1.parquet:/input_data/main_input_file.parquet,/tmp/dummy_step_python_pandas_results/:/results ./python-pandas-image.sif
```