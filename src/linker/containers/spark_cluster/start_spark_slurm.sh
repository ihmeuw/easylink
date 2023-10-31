#!/bin/bash
#start_spark_slurm.sh

#SBATCH --nodes=3
#  ntasks per node MUST be one, because multiple workers per node doesn't
#  work well with slurm + spark in this script (they would need increasing
#  ports among other things)
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=5
#SBATCH --mem-per-cpu=8000
#SBATCH --time=10:00:00

# NOTE: This script has a lot in it, and most of it is probably unnecessary.
# It is a heavily modified version of https://serverfault.com/a/776688

unset SPARK_HOME
CONDA_PATH=/opt/conda/condabin/conda # must be accessible within container
CONDA_ENV=spark_cluster
SINGULARITY_IMG=image.sif

export sparkLogs=$HOME/.spark_temp/logs
export sparkTmp=$HOME/.spark_temp/tmp

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

