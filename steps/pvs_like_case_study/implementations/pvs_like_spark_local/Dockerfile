# Stage 1: Start with the Apache Spark base image
FROM apache/spark as spark-base

# Stage 2: Start with the miniconda3 base image
FROM continuumio/miniconda3 as conda-base

# Setup I/O directories and copy the environment and Python script
RUN mkdir -p /input_data
RUN mkdir -p /results
VOLUME /results
VOLUME /input_data
COPY pvs_like_case_study_sample_data_spark_local.py pvs_like_case_study_spark_local_lock_no_jupyter.txt ./

# Create a new conda environment
RUN conda create -n pvs_like_case_study_spark_local --file=pvs_like_case_study_spark_local_lock_no_jupyter.txt

# Activate the conda environment and install any necessary Python packages
RUN echo "source activate pvs_like_case_study_spark_local" > ~/.bashrc
ENV PATH /opt/conda/envs/pvs_like_case_study_spark_local/bin:$PATH

# Final Stage: Copy from the previous stages
FROM spark-base

COPY --from=conda-base /opt/conda/envs/pvs_like_case_study_spark_local /opt/conda/envs/pvs_like_case_study_spark_local

# Set environment variables for conda
ENV PATH=/opt/conda/envs/pvs_like_case_study_spark_local/bin:${PATH}

# Run your script with Spark on startup
CMD ["/bin/bash", "-c", "conda run pvs_like_case_study_spark_local python myscript.py \
     && mv census_2030_with_piks_sample.parquet /results/"]