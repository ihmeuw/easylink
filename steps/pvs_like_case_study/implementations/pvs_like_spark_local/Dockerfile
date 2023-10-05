FROM apache/spark
RUN mkdir -p /input_data
RUN mkdir -p /results
VOLUME /results
VOLUME /input_data
COPY pvs_like_case_study_sample_data.py pvs_like_case_study_spark_local_lock_no_jupyter.txt ./

FROM continuumio/miniconda3
SHELL ["/bin/bash", "--login", "-c"]
RUN conda init bash \
    && . ~/.bashrc \
    && conda create -n pvs_like_case_study_spark_local --file=pvs_like_case_study_spark_local_lock_no_jupyter.txt \
    && conda activate pvs_like_case_study_spark_local
CMD ["bash", "-c", "conda run -n pvs_like_case_study_spark_local python pvs_like_case_study_sample_data_spark_local.py \
     && mv census_2030_with_piks_sample.parquet /results/"]