FROM python:3.11-slim
RUN mkdir -p /input_data
RUN mkdir -p /results
VOLUME /results
VOLUME /input_data
COPY pvs_like_case_study_r_lock.txt pvs_like_case_study_sample_data_r.py renv.lock ./
RUN conda create -n pvs_like_case_study_r --file=pvs_like_case_study_r_lock.txt
RUN conda activate pvs_like_case_study_r
RUN Rscript -e "renv::restore(library=.libPaths())"
CMD ["bash", "-c", \
    "python pvs_like_case_study_sample_data_r.py && \
    mv census_2030_with_piks_sample.parquet /results/"]
