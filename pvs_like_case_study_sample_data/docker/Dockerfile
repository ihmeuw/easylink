FROM python:3.11
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "pvs_like_case_study_sample_data.py", "&&", "cp", "census_2030_with_piks_sample.parquet", "/app/census_2030_with_piks_sample.parquet"]
