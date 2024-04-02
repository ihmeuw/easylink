import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional, TextIO, Tuple

from loguru import logger

from linker.configuration import Config


def is_on_slurm() -> bool:
    """Returns True if the current environment is a SLURM cluster."""
    return shutil.which("sbatch") is not None


def get_slurm_drmaa() -> "drmaa":
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # TODO [MIC-4469]: make more generic for external users
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa


def launch_slurm_job(
    session: "drmaa.Session",
    config: Config,
    container_engine: str,
    input_data: List[str],
    results_dir: Path,
    diagnostics_dir: Path,
    step_id: str,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
    implementation_config: Optional[Dict[str, str]] = None,
) -> None:
    """Runs a container as a job on a slurm cluster. The job is submitted via the
    `linker run-slurm-job` command line interface.
    """
    jt = _generate_job_template(
        session,
        config,
        container_engine,
        input_data,
        results_dir,
        diagnostics_dir,
        step_id,
        step_name,
        implementation_name,
        container_full_stem,
        implementation_config,
    )

    # Run the job
    job_id = session.runJob(jt)
    logger.debug("linker " + " ".join(jt.args))
    logger.info(
        f"Launching slurm job for step '{step_name}', implementation '{implementation_name}\n"
        f"Job submitted with jobid '{job_id}'\n"
        f"Output log: {str(diagnostics_dir / f'{job_id}.o*')}\n"
        f"Error log: {str(diagnostics_dir / f'{job_id}.e*')}"
    )
    job_status = session.wait(job_id, session.TIMEOUT_WAIT_FOREVER)

    # TODO: clean up if job failed?
    logger.info(f"Job {job_id} finished with status '{job_status}'")
    session.deleteJobTemplate(jt)


def submit_spark_cluster_job(
    drmaa: "drmaa",
    session: "drmaa.Session",
    config: Config,
    launcher: TextIO,
    diagnostics_dir: Path,
    step_id: str,
) -> Tuple[Path, str]:
    """Submits a job to launch a Spark cluster.

    Args:
        drmaa: DRMAA module.
        session: DRMAA session.
        config: Config object.
        launcher: Launcher script.
        diagnostics_dir: Diagnostics directory.
        step_id: Step ID used for naming the job.

    Returns:
        Path to stderr log, which contains the Spark master URL.
        Main job ID of the spark cluster.
    """
    jt, resources = _generate_spark_cluster_job_template(
        session, config, launcher, diagnostics_dir, step_id
    )
    jobs = session.runBulkJobs(jt, 1, resources["num_workers"] + 1, 1)
    error_logs = [Path(jt.workingDirectory) / f"spark_cluster_{job}.stderr" for job in jobs]
    output_logs = [Path(jt.workingDirectory) / f"spark_cluster_{job}.stdout" for job in jobs]
    master_error_log = error_logs[0]

    logger.info(
        f"Submitting slurm job for launching the Spark cluster: '{jt.jobName}'\n"
        f"Job submitted with jobids '{jobs}' to execute script '{launcher.name}'\n"
        f"Master error log: {master_error_log}"
    )
    logger.debug(
        f"Output logs: {[str(o) for o in output_logs]}\n"
        f"Error logs: {[str(e) for e in error_logs]}"
    )

    # Wait for job to start running
    job_statuses = [session.jobStatus(job_id) == drmaa.JobState.RUNNING for job_id in jobs]
    while not all(job_statuses):
        sleep(5)
        logger.debug("Waiting for jobs to start running...")
        job_statuses = [
            session.jobStatus(job_id) == drmaa.JobState.RUNNING for job_id in jobs
        ]
    logger.info(f"Jobs {jobs} are running")

    session.deleteJobTemplate(jt)
    return master_error_log, jobs[0].split("_")[0]


####################
# Helper functions #
####################


def _get_cli_args(job_name, account, partition, peak_memory, max_runtime, num_threads):
    return (
        f"-J {job_name} "
        f"-A {account} "
        f"-p {partition} "
        f"--mem={peak_memory*1024} "
        f"-t {max_runtime}:00:00 "
        f"-c {num_threads}"
    )


def _generate_job_template(
    session: "drmaa.Session",
    config: Config,
    container_engine: str,
    input_data: List[str],
    results_dir: Path,
    diagnostics_dir: Path,
    step_id: str,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
    implementation_config: Optional[Dict[str, str]],
) -> "drmaa.session.JobTemplate":
    jt = session.createJobTemplate()
    jt.jobName = f"{implementation_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(diagnostics_dir / '%A.o%a')}"
    jt.errorPath = f":{str(diagnostics_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    jt_args = [
        "run-slurm-job",
        container_engine,
        str(results_dir),
        str(diagnostics_dir),
        step_id,
        step_name,
        implementation_name,
        container_full_stem,
        "-vvv",
    ]
    for filepath in input_data:
        jt_args.extend(("--input-data", str(filepath)))
    if implementation_config:
        jt_args.extend(("--implementation-config", str(json.dumps(implementation_config))))
    jt.args = jt_args
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    resources = config.slurm_resources
    jt.nativeSpecification = _get_cli_args(
        job_name=jt.jobName,
        account=resources["account"],
        partition=resources["partition"],
        peak_memory=resources["memory"],
        max_runtime=resources["time_limit"],
        num_threads=resources["cpus"],
    )

    return jt


def _generate_spark_cluster_job_template(
    session: "drmaa.session",
    config: Config,
    launcher: TextIO,
    diagnostics_dir: Path,
    step_id: str,
):
    jt = session.createJobTemplate()
    jt.jobName = f"spark_cluster_{step_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.workingDirectory = str(diagnostics_dir)
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(Path(jt.workingDirectory) / 'spark_cluster_%A_%a.stdout')}"
    jt.errorPath = f":{str(Path(jt.workingDirectory) / 'spark_cluster_%A_%a.stderr')}"
    jt.remoteCommand = shutil.which("/bin/bash")
    jt.args = [launcher.name]
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    resources = config.spark_resources
    jt.nativeSpecification = (
        f"--account={resources['account']} "
        f"--partition={resources['partition']} "
        f"--mem={resources['mem_per_node'] * 1024} "
        f"--time={resources['time_limit']}:00:00 "
        f"--cpus-per-task={resources['cpus_per_node']}"
    )

    return jt, resources
