FROM apache/spark-py@sha256:489f904a77f21134df4840de5f8bd9f110925e7b439ca6a04b7c033813edfebc

USER root
# pandas is used to write out a single file
RUN pip3 install numpy pandas==2.1.2 pyarrow pyyaml jellyfish splink==3.9.14
# NOTE: I don't understand why, but in the base image, it has pyspark installed somewhere
# that isn't on PYTHONPATH by default
# I looked in /opt/spark/bin/pyspark to find this
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
RUN mkdir -p /input_data
RUN mkdir -p /results
RUN mkdir -p /diagnostics
RUN mkdir -p /workdir
RUN mkdir -p /code
COPY cascade_clustering_spark.py /code/
COPY splink_model_params.json /code/
VOLUME /results
VOLUME /diagnostics
VOLUME /input_data
# Annoyingly, Spark _requires_ write access to the working directory, to write a file called java_opts.txt
WORKDIR /workdir
ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/code/cascade_clustering_spark.py"]