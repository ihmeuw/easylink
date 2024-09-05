FROM python:3.11-slim
RUN mkdir -p /input_data
RUN mkdir -p /extra_implementation_specific_input_data
RUN mkdir -p /results
RUN mkdir -p /diagnostics
VOLUME /results
VOLUME /diagnostics
VOLUME /input_data
RUN pip install numpy pandas==2.1.2 pyarrow pyyaml jellyfish splink==3.9.14
COPY cascade_clustering.py .
COPY splink_model_params.json .

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["cascade_clustering.py"]