FROM python:3.11-slim
RUN mkdir -p /input_data
RUN mkdir -p /results
RUN mkdir -p /diagnostics
VOLUME /results
VOLUME /input_data
VOLUME /diagnostics
COPY pvs_like_case_study_sample_data.py requirements.txt ./
RUN pip install -r requirements.txt
CMD ["bash", "-c", "python pvs_like_case_study_sample_data.py"]
