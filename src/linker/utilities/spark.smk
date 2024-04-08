rule start_spark_master:
    output:
        rules.all.input.master_log,
    container:
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images/spark_cluster.sif"
    params:
        terminate_file_name=rules.all.input.term,
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
    echo "Master PID is" $spid

    trap "kill -9 $spid &>/dev/null" EXIT

    while true; do

        if ! kill -0 $spid >/dev/null 2>&1; then
            echo "rule $spid died"
            cat {output} >&2
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
        temp(config["results_dir"] + "/spark_logs/spark_master_uri.txt"),
    params:
        spark_master_log_file=rules.all.input.master_log,
    localrule: True
    shell:
        """
        while true; do
            echo "Waiting for Spark Cluster to start..."

            if [[ -e {params.spark_master_log_file} ]]; then
                found=`grep -o "\\(spark://.*\$\\)" {params.spark_master_log_file} || true`

                if [[ ! -z $found ]]; then
                    echo $found > {output}
                    break
                fi
            fi

            sleep 10

        done
        echo "Quitting Master wait loop"
        """


rule split_workers:
    input:
        rules.wait_for_spark_master.output,
    output:
        temp(
            touch(
                scatter.num_workers(
                    config["results_dir"] + "/spark_logs/spark_worker_{scatteritem}.txt"
                )
            )
        ),


rule start_spark_worker:
    input:
        masterurl=rules.wait_for_spark_master.output,
        workers=config["results_dir"] + "/spark_logs/spark_worker_{scatteritem}.txt",
    output:
        config["results_dir"] + "/spark_logs/spark_worker_log_{scatteritem}.txt",
    params:
        terminate_file_name=rules.all.input.term,
        user=os.environ["USER"],
    container:
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images/spark_cluster.sif"
    shell:
        """
        echo " Starting Spark Worker"
        SPARK_ROOT=/opt/spark
        SPARK_WORKER_WEBUI_PORT=28510
        read -r MASTER_URL < {input.masterurl}

         rm -f {output} || true
         mkdir -p "/tmp/spark_cluster_{params.user}"

        $SPARK_ROOT/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --cores 1 --memory 1G \
        --webui-port $SPARK_WORKER_WEBUI_PORT \
        --work-dir /tmp/singularity_spark_{params.user}/spark_work \
        $MASTER_URL \
        &> {output} &
    spid=$!
    echo "Worker PID is" $spid

    trap "kill -9 $spid &>/dev/null" EXIT

    while true; do

        if ! kill -0 $spid >/dev/null 2>&1; then
            echo "rule $spid died"
            cat {output} >&2
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
        temp(config["results_dir"] + "/spark_logs/spark_worker_started_{scatteritem}.txt"),
    params:
        spark_worker_log_file=config["results_dir"]
        + "/spark_logs/spark_worker_log_{scatteritem}.txt",
    localrule: True
    shell:
        """
        read -r MASTER_URL < {input}
        while true; do
            echo "Waiting for Spark Worker to start..."

            if [[ -e {params.spark_worker_log_file} ]]; then
                found=`grep -o "\\(Worker: Successfully registered with master $MASTER_URL\\)" {params.spark_worker_log_file} || true`

                if [[ ! -z $found ]]; then
                    echo $found
                    touch {output}
                    break
                fi
            fi

            sleep 10

        done
        echo "Quitting Worker wait loop"
        """


rule terminate_spark:
    input:
        rules.all.input.final_output,
    output:
        temp(rules.all.input.term),
    localrule: True
    shell:
        """
        touch {output}
        """
