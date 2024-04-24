rule terminate_spark:
    input:
        "result.parquet",
    output:
        temp("spark_logs/spark_master_terminated.txt"),
    localrule: True
    shell:
        """
        touch {output}
        """


rule start_spark_master:
    output:
        "spark_logs/spark_master_log.txt",
    container:
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images/spark_cluster.sif"
    params:
        terminate_file_name=rules.terminate_spark.output,
    shell:
        """
    echo "Starting spark master - logging to {output}"
    SPARK_ROOT=/opt/spark
    SPARK_MASTER_PORT=28508
    SPARK_MASTER_WEBUI_PORT=28509
    SPARK_MASTER_HOST=$(hostname -f)


    rm -f {output} || true

    $SPARK_ROOT/bin/spark-class org.apache.spark.deploy.master.Master \
    --host $SPARK_MASTER_HOST \
    --port $SPARK_MASTER_PORT \
    --webui-port $SPARK_MASTER_WEBUI_PORT \
    &> {output} &
    spid=$!

    trap "kill -9 $spid &>/dev/null" EXIT

    while true; do

        if ! kill -0 $spid >/dev/null 2>&1; then
            echo "rule $spid died"
          #  cat {output} >&2
            exit 1
        fi

        if [[ -e "{params.terminate_file_name}" ]] ; then
            break
        fi

        sleep 1
    done
    """


rule wait_for_spark_master:
    output:
        temp("spark_logs/spark_master_uri.txt"),
    params:
        spark_master_log_file=rules.start_spark_master.output,
        max_attempts=20,
        attempt_sleep_time=10,
    localrule: True
    shell:
        """
        echo "Searching for Spark master URL in {params.spark_master_log_file}"
        sleep {params.attempt_sleep_time}
        num_attempts=1
        while true; do

            if [[ -e {params.spark_master_log_file} ]]; then
                found=`grep -o "\(spark://.*$\)" {params.spark_master_log_file} || true`

                if [[ ! -z $found ]]; then
                    echo "Spark master URL found: $found"
                    echo $found > {output}
                    break
                fi
            fi

            if (( $num_attempts > {params.max_attempts})); then
                echo "Couldn't find Spark master after {params.max_attempts} attempts. Exiting."
                exit 2
            
            fi

            echo "Unable to find Spark master URL in logfile. Waiting {params.attempt_sleep_time} seconds and retrying..."
            echo "(attempt $num_attempts/{params.max_attempts})"
            sleep {params.attempt_sleep_time}
            num_attempts=$((num_attempts + 1))

        done
        """


rule split_workers:
    localrule: True
    input:
        rules.wait_for_spark_master.output,
    output:
        temp(
            touch(
                scatter.num_workers(
                    "spark_logs/spark_worker_{scatteritem}.txt"
                )
            )
        ),


rule start_spark_worker:
    input:
        masterurl=rules.wait_for_spark_master.output,
        workers="spark_logs/spark_worker_{scatteritem}.txt",
    output:
        "spark_logs/spark_worker_log_{scatteritem}.txt",
    params:
        terminate_file_name=rules.terminate_spark.output,
        user=os.environ["USER"],
        cores=1,
        memory=1024,

    container:
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images/spark_cluster.sif"
    shell:
        """
        echo " Starting Spark Worker {wildcards.scatteritem}"
        SPARK_ROOT=/opt/spark
        SPARK_WORKER_WEBUI_PORT=28510
        read -r MASTER_URL < {input.masterurl}

         rm -f {output} || true
         mkdir -p "/tmp/spark_cluster_{params.user}"

        $SPARK_ROOT/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --cores {params.cores} --memory '{params.memory}M' \
        --webui-port $SPARK_WORKER_WEBUI_PORT \
        --work-dir /tmp/singularity_spark_{params.user}/spark_work \
        $MASTER_URL \
        &> {output} &
    spid=$!

    trap "kill -9 $spid &>/dev/null" EXIT

    while true; do

        if ! kill -0 $spid >/dev/null 2>&1; then
            echo "rule $spid died"
           # cat {output} >&2
            exit 1
        fi

        if [[ -e "{params.terminate_file_name}" ]] ; then
            break
        fi

        sleep 1
    done
        """


rule wait_for_spark_worker:
    input:
        rules.wait_for_spark_master.output,
    output:
        temp("spark_logs/spark_worker_started_{scatteritem}.txt"),
    params:
        spark_worker_log_file="spark_logs/spark_worker_log_{scatteritem}.txt",
        max_attempts=20,
        attempt_sleep_time=20,
    localrule: True
    shell:
        """
        echo "Waiting for Spark Worker {wildcards.scatteritem} to start..."
        sleep {params.attempt_sleep_time}
        num_attempts=1
        read -r MASTER_URL < {input}
        while true; do

            if [[ -e {params.spark_worker_log_file} ]]; then
                found=`grep -o "\(Worker: Successfully registered with master $MASTER_URL\)" {params.spark_worker_log_file} || true`

                if [[ ! -z $found ]]; then
                    echo "Spark Worker {wildcards.scatteritem} registered successfully"
                    touch {output}
                    break
                fi
            fi

            if (( $num_attempts > {params.max_attempts})); then
                echo "Couldn't find Spark worker {wildcards.scatteritem} after {params.max_attempts} attempts. Exiting."
                exit 2
            
            fi
            
            echo "Unable to find Spark worker {wildcards.scatteritem} registration. Waiting {params.attempt_sleep_time} seconds and retrying..."
            echo "(attempt $num_attempts/{params.max_attempts})"
            sleep {params.attempt_sleep_time}
            num_attempts=$((num_attempts + 1))

        done
        """
