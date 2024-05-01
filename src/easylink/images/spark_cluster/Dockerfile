# Stage 1: Start with the miniconda3 base image
FROM continuumio/miniconda3 as conda-base

# Create a new conda environment
SHELL ["/bin/bash", "--login", "-c"]
RUN conda init bash \
    && . ~/.bashrc \
    && conda create -n spark_cluster python=3.10

# Stage 2: Start with the Apache Spark base image
FROM apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63 as spark-base

COPY --from=conda-base /opt/conda /opt/conda

# Set PATH for conda environment and conda itself
ENV PATH=/opt/conda/envs/spark_cluster/bin:/opt/conda/condabin:${PATH}
