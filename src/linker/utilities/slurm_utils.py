import atexit
import os
import shutil
import types
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, TextIO

from loguru import logger


def get_slurm_drmaa() -> types.ModuleType("drmaa"):
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # TODO [MIC-4469]: make more generic for external users
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa


def launch_slurm_job(
    session: types.ModuleType("drmaa.Session"),
    resources: Dict[str, str],
    container_engine: str,
    input_data: List[str],
    results_dir: Path,
    diagnostics_dir: Path,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
) -> None:
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
        step_name,
        implementation_name,
        container_full_stem,
        "-vvv",
    ]
    for filepath in input_data:
        jt_args.extend(("--input-data", str(filepath)))
    jt.args = jt_args
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    jt.nativeSpecification = get_cli_args(
        job_name=jt.jobName,
        account=resources["account"],
        partition=resources["partition"],
        peak_memory=resources["memory"],
        max_runtime=resources["time_limit"],
        num_threads=resources["cpus"],
    )

    # Run the job
    job_id = session.runJob(jt)
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


def get_cli_args(job_name, account, partition, peak_memory, max_runtime, num_threads):
    return (
        f"-J {job_name} "
        f"-A {account} "
        f"-p {partition} "
        f"--mem={peak_memory*1024} "
        f"-t {max_runtime}:00:00 "
        f"-c {num_threads}"
    )


def submit_spark_cluster_job(
    session: types.ModuleType("drmaa.Session"),
    launcher: TextIO,
    account: str,
    partition: str,
    memory_per_cpu: int,
    max_runtime: int,
    num_workers: int,
    cpus_per_task: int,
    preserve_logs: bool = False,
) -> Path:
    """Submits a job to launch a Spark cluster.

    Args:
        session: DRMAA session.
        launcher: Launcher script.
        account: Account to charge.
        partition: Partition to run on.
        memory_per_cpu: Memory per node in GB.
        max_runtime: Maximum runtime in hours.
        num_workers: Number of workers.
        cpus_per_task: Number of CPUs per task.
        preserve_logs: Whether to preserve logs.

    Returns:
        Path to stderr log, which contains the Spark master URL.
    """
    jt = session.createJobTemplate()
    jt.jobName = f"spark_cluster_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.workingDirectory = os.getcwd()
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(Path(jt.workingDirectory) / '%A_%a.stdout')}"
    jt.errorPath = f":{str(Path(jt.workingDirectory) / '%A_%a.stderr')}"
    jt.remoteCommand = shutil.which("/bin/bash")
    jt.args = [launcher.name]
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    jt.nativeSpecification = (
        f"--account={account} "
        f"--partition={partition} "
        f"--mem-per-cpu={memory_per_cpu * 1024} "
        f"--time={max_runtime}:00:00 "
        f"--nodes=1 "
        f"--cpus-per-task={cpus_per_task} "
        "--ntasks-per-node=1"
    )
    jobs = session.runBulkJobs(jt, 1, num_workers + 1, 1)
    error_logs = [Path(jt.workingDirectory) / f"{job}.stderr" for job in jobs]
    output_logs = [Path(jt.workingDirectory) / f"{job}.stdout" for job in jobs]
    master_error_log = error_logs[0]

    logger.info(
        f"Submitting slurm job for launching the Spark cluster: '{jt.jobName}'\n"
        f"Job submitted with jobids '{jobs}' to execute script '{launcher.name}'\n"
        f" Master error log: {master_error_log}\n"
    )
    logger.debug(
        f" Output logs: {[str(o) for o in output_logs]}\n"
        f" Error logs: {[str(e) for e in error_logs]}"
    )

    if not preserve_logs:
        for output_log in output_logs:
            atexit.register(lambda: os.remove(output_log))
        for error_log in error_logs:
            atexit.register(lambda: os.remove(error_log))

    # Wait for job to start running
    drmaa = get_slurm_drmaa()
    job_statuses = [session.jobStatus(job_id) == drmaa.JobState.RUNNING for job_id in jobs]
    while all(job_statuses):
        sleep(5)
        logger.debug("Waiting for jobs to start running...")
        job_statuses = [
            session.jobStatus(job_id) == drmaa.JobState.RUNNING for job_id in jobs
        ]
    logger.info(f"Jobs {jobs} are running")

    session.deleteJobTemplate(jt)
    session.exit()
    return master_error_log
