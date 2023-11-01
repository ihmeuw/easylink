import atexit
import os
from pathlib import Path
import tempfile

from typing import TextIO

from linker.utilities.slurm_utils import submit_spark_cluster_job

CONDA_PATH="/ihme/homes/mkappel/miniconda3/condabin/conda "# must be accessible within container
CONDA_ENV="pvs_like_case_study_spark_node"
SINGULARITY_IMG="docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63"

def build_cluster():
    """Builds a Spark cluster.

    Returns:
        spark_master_url: Spark master URL.
    """
    spark_master_url = ""

    # call build_launch_script
    launcher = build_cluster_launch_script()

    # submit job


    # grep log for spark master url or is there a better approach?

    return spark_master_url


def build_cluster_launch_script(
    worker_settings_file: Path,
    worker_log_directory: Path,
) -> TextIO:
    """Generates a shell file that, on execution, spins up a Spark cluster."""
    launcher = tempfile.NamedTemporaryFile(
        mode="w",
        dir=".",
        prefix="spark_cluster_launcher_",
        suffix=".sh",
        delete=False,
    )

    output_dir = str(worker_settings_file.resolve().parent)
    # TODO: handle .cluster.ihme.washington.edu
    launcher.write(
        f"""
#!/bin/bash

unset SPARK_HOME
CONDA_PATH=/opt/conda/condabin/conda # must be accessible within container
CONDA_ENV=spark_cluster
SINGULARITY_IMG=image.sif

export sparkLogs=$HOME/.spark_temp/logs
export SPARK_ROOT=/opt/spark # within the container
export SPARK_WORKER_DIR=$sparkLogs
export SPARK_LOCAL_DIRS=$sparkLogs
export SPARK_MASTER_PORT=28508
export SPARK_MASTER_WEBUI_PORT=28509
export SPARK_WORKER_CORES=$SLURM_CPUS_PER_TASK
export SPARK_DAEMON_MEMORY=$(( $SLURM_MEM_PER_CPU * $SLURM_CPUS_PER_TASK / 2 ))m
export SPARK_MEM=$SPARK_DAEMON_MEMORY

# This section will be run when started by sbatch
if [ "$1" != 'multi_job' ]; then
    this=$0
    # I experienced problems with some nodes not finding the script:
    #   slurmstepd: execve(): /var/spool/slurm/job123/slurm_script:
    #   No such file or directory
    # that's why this script is being copied to a shared location to which
    # all nodes have access:
    mkdir -p $HOME/.spark_temp
    script=$HOME/.spark_temp/${SLURM_JOBID}_$( basename -- "$0" )
    cp "$this" "$script"

    srun $script 'multi_job'
# If run by srun, then decide by $SLURM_PROCID whether we are master or worker
else
    if [ "$SLURM_PROCID" -eq 0 ]; then
        export SPARK_MASTER_IP="$(hostname).cluster.ihme.washington.edu"
        MASTER_NODE=$( scontrol show hostname $SLURM_NODELIST | head -n 1 )

        mkdir -p /tmp/pvs_like_case_study_spark_local_$USER
        singularity exec -B /mnt:/mnt,/tmp/pvs_like_case_study_spark_local_$USER:/tmp $SINGULARITY_IMG $CONDA_PATH run --no-capture-output -n $CONDA_ENV "$SPARK_ROOT/bin/spark-class" org.apache.spark.deploy.master.Master --host "$SPARK_MASTER_IP" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT"
    else
        # $(scontrol show hostname) is used to convert e.g. host20[39-40]
        # to host2039.
        # TODO: This step assumes that SLURM_PROCID=0 corresponds to the first node in SLURM_NODELIST. Is this reasonable?
        MASTER_NODE=spark://$( scontrol show hostname $SLURM_NODELIST | head -n 1 ):$SPARK_MASTER_PORT

        mkdir -p /tmp/pvs_like_case_study_spark_local_$USER
        singularity exec -B /mnt:/mnt,/tmp/pvs_like_case_study_spark_local_$USER:/tmp $SINGULARITY_IMG $CONDA_PATH run --no-capture-output -n $CONDA_ENV "$SPARK_ROOT/bin/spark-class" org.apache.spark.deploy.worker.Worker $MASTER_NODE
    fi
fi
    """
    )
    launcher.close()

    atexit.register(lambda: os.remove(launcher.name))
    return launcher


