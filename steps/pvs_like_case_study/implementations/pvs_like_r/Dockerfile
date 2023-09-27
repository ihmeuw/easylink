FROM continuumio/miniconda3
RUN mkdir -p /input_data
RUN mkdir -p /results
VOLUME /results
VOLUME /input_data
COPY pvs_like_case_study_r_lock.txt pvs_like_case_study_sample_data_r.py renv.lock ./
SHELL ["/bin/bash", "--login", "-c"]
RUN conda init bash \
    && . ~/.bashrc \
    && conda create -n pvs_like_case_study_r --file=pvs_like_case_study_r_lock.txt \
    && conda activate pvs_like_case_study_r \
    && Rscript -e 'renv::restore(library=.libPaths())' \
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bash_profile
RUN echo "conda activate pvs_like_case_study_r" >> ~/.bash_profile
CMD ["bash", "-c", "python pvs_like_case_study_sample_data_r.py \
     && mv census_2030_with_piks_sample.parquet /results/"]
