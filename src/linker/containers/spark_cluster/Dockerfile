# Stage 1: Start with the miniconda3 base image
FROM continuumio/miniconda3 as conda-base

## Setup I/O directories and copy the environment and Python script
#RUN mkdir -p /input_data
#RUN mkdir -p /results
#VOLUME /results
#VOLUME /input_data
#COPY pvs_like_case_study_sample_data_spark_local.py pvs_like_case_study_spark_local_lock_no_jupyter.txt ./

# Create a new conda environment
SHELL ["/bin/bash", "--login", "-c"]
RUN conda init bash \
    && . ~/.bashrc \
    && conda create -n spark_cluster python=3.10

# Stage 2: Start with the Apache Spark base image
FROM apache/spark as spark-base

COPY --from=conda-base /opt/conda /opt/conda
#COPY --from=conda-base pvs_like_case_study_sample_data_spark_local.py ./

# Set PATH for conda environment and conda itself
ENV PATH=/opt/conda/envs/spark_cluster/bin:/opt/conda/condabin:${PATH}

# Run your script with Spark on startup
# TODO: update to use slurm to build the cluster
CMD ["/bin/bash", "-c", "conda run -n spark_cluster python /opt/spark/work-dir/pvs_like_case_study_sample_data_spark_local.py \
     && mv census_2030_with_piks_sample.parquet /results/"]
