FROM python:3.11
WORKDIR /app
COPY . .
# TODO: Do not copy everything b/c we want users to provide input data
RUN pip install -r requirements.txt
CMD ["python", "pvs_like_case_study_sample_data.py"]
