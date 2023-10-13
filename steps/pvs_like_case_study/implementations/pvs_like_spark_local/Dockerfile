FROM apache/spark

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD *.py *.txt /app/

# Install wget and bzip2
#RUN apt-get update -y && apt-get upgrade -y
#RUN apt-get install -y wget bzip2

# Install miniconda to /miniconda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
RUN bash ~/miniconda.sh -b -p /miniconda
ENV PATH="/miniconda/bin:${PATH}"
RUN conda update -y conda

# Create a conda environment based on myenvironment.txt
COPY pvs_like_case_study_spark_local_lock_no_jupyter.txt /app/pvs_like_case_study_spark_local_lock_no_jupyter.txt
COPY pvs_like_case_study_sample_data_spark_local.py /app/pvs_like_case_study_sample_data_spark_local.py
RUN conda create -n pvs_like_case_study_spark_local -f pvs_like_case_study_spark_local_lock_no_jupyter.txt

# Activate the environment
RUN echo "source activate pvs_like_case_study_spark_local" > ~/.bashrc
ENV PATH /opt/conda/envs/pvs_like_case_study_spark_local/bin:$PATH

# Run command from conda environment
CMD /bin/bash -c "source activate pvs_like_case_study_spark_local && python pvs_like_case_study_sample_data_spark_local.py"