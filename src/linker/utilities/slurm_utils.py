
import os
import shutil
import types
from pathlib import Path

from datetime import datetime
from typing import Dict, List

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
    input_data: List[Path],
    results_dir: Path,
    step_name: str,
    implementation_name: str,
    implementation_dir: Path,
    container_full_stem: str,
) -> None:
    jt = session.createJobTemplate()
    jt.jobName = f"{step_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(results_dir / '%A.o%a')}"
    jt.errorPath = f":{str(results_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    jt_args = [
        "run-slurm-job",
        container_engine,
        str(results_dir),
        step_name,
        implementation_name,
        str(implementation_dir),
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
        f"Output log: {str(results_dir / f'{job_id}.o*')}\n"
        f"Error log: {str(results_dir / f'{job_id}.e*')}"
    )
    job_status = session.wait(job_id, session.TIMEOUT_WAIT_FOREVER)

    # TODO: clean up if job failed?
    logger.info(f"Job {job_id} finished with status '{job_status}'")
    session.deleteJobTemplate(jt)
    session.exit()


def get_slurm_drmaa() -> types.ModuleType("drmaa"):
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # TODO [MIC-4469]: make more generic for external users
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa


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
    resources: Dict[str, str],
    container_engine: str,
    input_data: List[Path],
    results_dir: Path,
    step_name: str,
    step_dir: Path,
) -> None:
    jt = session.createJobTemplate()
    jt.jobName = f"spark_cluster_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(results_dir / '%A.o%a')}"
    jt.errorPath = f":{str(results_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    jt_args = [
        "build-spark-cluster",
        container_engine,
        str(results_dir),
        step_name,
        str(step_dir),
        "-vvv",
    ]
    # for filepath in input_data:
    #     jt_args.extend(("--input-data", str(filepath)))
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
    job_id = session.runJob(jt)
    logger.info(
        f"Launching slurm job for step '{step_name}'\n"
        f"Job submitted with jobid '{job_id}'\n"
        f"Output log: {str(results_dir / f'{job_id}.o*')}\n"
        f"Error log: {str(results_dir / f'{job_id}.e*')}"
    )
    job_status = session.wait(job_id, session.TIMEOUT_WAIT_FOREVER)
    # TODO: clean up if job failed?
    logger.info(f"Job {job_id} finished with status '{job_status}'")
    session.deleteJobTemplate(jt)
    session.exit()