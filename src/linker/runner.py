import os
import shutil
import socket
import types
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, List

from loguru import logger

from linker.configuration import Config
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.singularity_utils import run_with_singularity


def main(
    config: Config,
    results_dir: Path,
) -> None:
    if config.computing_environment == "local":
        runner = run_container
    elif config.computing_environment == "slurm":
        # TODO [MIC-4468]: Check for slurm in a more meaningful way
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        drmaa = _get_slurm_drmaa()
        session = drmaa.Session()
        session.initialize()
        resources = config.get_resources()
        runner = partial(launch_slurm_job, session, resources)
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )
    for step_name in config.steps:
        step_dir = config.get_step_directory(step_name)
        runner(config.container_engine, config.input_data, results_dir, step_name, step_dir)


def run_container(
    container_engine: str,
    input_data: List[str],
    results_dir: Path,
    step_name: str,
    step_dir: Path,
) -> None:
    # TODO: send error to stdout in the event the step script fails
    #   (currently it's only logged in the .o file)
    logger.info(f"Running step: '{step_name}'")
    if container_engine == "docker":
        run_with_docker(input_data, results_dir, step_dir)
    elif container_engine == "singularity":
        run_with_singularity(input_data, results_dir, step_dir)
    else:
        if container_engine:
            logger.warning(
                "The container engine is expected to be either 'docker' or "
                f"'singularity' but got '{container_engine}' - trying to run "
                "with Docker and then (if that fails) Singularity."
            )
        else:
            logger.info(
                "No container engine is specified - trying to run with Docker and "
                "then (if that fails) Singularity."
            )
        try:
            run_with_docker(input_data, results_dir, step_dir)
        except Exception as e_docker:
            logger.warning(f"Docker failed with error: '{e_docker}'")
            try:
                run_with_singularity(input_data, results_dir, step_dir)
            except Exception as e_singularity:
                raise RuntimeError(
                    f"Both docker and singularity failed:\n"
                    f"    Docker error: {e_docker}\n"
                    f"    Singularity error: {str(e_singularity)}"
                )


def launch_slurm_job(
    session: types.ModuleType("drmaa.Session"),
    resources: Dict[str, str],
    container_engine: str,
    input_data: List[Path],
    results_dir: Path,
    step_name: str,
    step_dir: Path,
) -> None:
    jt = session.createJobTemplate()
    jt.jobName = f"{step_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(results_dir / '%A.o%a')}"
    jt.errorPath = f":{str(results_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    args = [
        "run-slurm-job",
        container_engine,
        str(results_dir),
        step_name,
        str(step_dir),
        "-vvv",
    ]
    for filepath in input_data:
        args.append("--input-data")
        args.append(f"{str(filepath)}")
    jt.args = args
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    jt.nativeSpecification = _get_cli_args(
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


def _get_slurm_drmaa() -> types.ModuleType("drmaa"):
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # TODO [MIC-4469]: make more generic for external users
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa


def _get_cli_args(job_name, account, partition, peak_memory, max_runtime, num_threads):
    return (
        f"-J {job_name} "
        f"-A {account} "
        f"-p {partition} "
        f"--mem={peak_memory*1024} "
        f"-t {max_runtime}:00:00 "
        f"-c {num_threads}"
    )
