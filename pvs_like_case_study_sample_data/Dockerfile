FROM python:3.11-slim
WORKDIR /app
COPY pvs_like_case_study_sample_data.py requirements.txt ./
RUN pip install -r requirements.txt
CMD ["bash", "-c", \
    "python pvs_like_case_study_sample_data.py && \
    mv census_2030_with_piks_sample.parquet /app/results/"]