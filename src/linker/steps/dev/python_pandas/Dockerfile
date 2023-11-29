# https://hub.docker.com/layers/library/python/3.10/images/sha256-1c26c25390307b64e8ff73e7edf34b4fbeac59d41da41c08da28dc316a721899?context=explore
FROM python@sha256:1c26c25390307b64e8ff73e7edf34b4fbeac59d41da41c08da28dc316a721899
RUN mkdir -p /input_data
RUN mkdir -p /extra_implementation_specific_input_data
RUN mkdir -p /results
RUN mkdir -p /extra_implementation_specific_results
VOLUME /results
VOLUME /input_data
RUN pip install pandas==2.1.2 pyarrow
COPY dummy_step.py .

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["dummy_step.py"]