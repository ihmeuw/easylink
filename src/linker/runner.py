import os
import shutil
import socket
import types
from datetime import datetime
from pathlib import Path
from typing import Dict, NamedTuple, Union

from loguru import logger

from linker.utilities.docker_utils import run_with_docker
from linker.utilities.pipeline_utils import get_steps
from linker.utilities.singularity_utils import run_with_singularity


def main(
    pipeline_specification: Path,
    container_engine: str,
    compute_config: Dict[str, Union[str, Dict]],
    results_dir: Path,
) -> None:
    step_dir = get_steps(pipeline_specification)

    if compute_config["computing_environment"] == "local":
        _run_container(container_engine, results_dir, step_dir)
    elif compute_config["computing_environment"] == "slurm":
        # TODO [MIC-4468]: Check for slurm in a more meaningful way
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        launch_slurm_job(
            pipeline_specification,
            container_engine,
            results_dir,
            compute_config,
        )

    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {compute_config['computing_environment']}"
        )


def _run_container(container_engine: str, results_dir: Path, step_dir: Path):
    if container_engine == "docker":
        run_with_docker(results_dir, step_dir)
    elif container_engine == "singularity":
        run_with_singularity(results_dir, step_dir)
    else:  # "unknown"
        try:
            run_with_docker(results_dir, step_dir)
        except Exception as e_docker:
            logger.warning(f"Docker failed with error: '{e_docker}'")
            try:
                run_with_singularity(results_dir, step_dir)
            except Exception as e_singularity:
                raise RuntimeError(
                    f"Both docker and singularity failed:\n"
                    f"    Docker error: {e_docker}\n"
                    f"    Singularity error: {str(e_singularity)}"
                )


class NativeSpecification(NamedTuple):
    job_name: str
    account: str
    partition: str
    peak_memory: int  # GB
    max_runtime: int  # hours
    num_threads: int

    def to_cli_args(self):
        return (
            f"-J {self.job_name} "
            f"-A {self.account} "
            f"-p {self.partition} "
            f"--mem={self.peak_memory*1024} "
            f"-t {self.max_runtime}:00:00 "
            f"-c {self.num_threads}"
        )


def launch_slurm_job(
    pipeline_specification: Path,
    container_engine: str,
    results_dir: Path,
    compute_config: Dict[str, Dict],
) -> None:
    resources = {
        **compute_config["implementation_resources"],
        **compute_config["slurm"],
    }
    drmaa = _get_slurm_drmaa()
    s = drmaa.Session()
    s.initialize()
    jt = s.createJobTemplate()
    jt.jobName = f"linker_run_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(results_dir / '%A.o%a')}"
    jt.errorPath = f":{str(results_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    jt.args = [
        "run-slurm-job",
        str(pipeline_specification),
        container_engine,
        str(results_dir),
        "-vvv",
    ]
    jt.jobEnvironment = {
        "LC_ALL": "en_US.UTF-8",
        "LANG": "en_US.UTF-8",
    }
    native_specification = NativeSpecification(
        job_name=jt.jobName,
        account=resources["account"],
        partition=resources["partition"],
        peak_memory=resources["memory"],
        max_runtime=resources["time_limit"],
        num_threads=resources["cpus"],
    )
    jt.nativeSpecification = native_specification.to_cli_args()
    job_id = s.runJob(jt)
    logger.info(f"Job submitted with jobid '{job_id}'")
    s.deleteJobTemplate(jt)
    s.exit()


def _get_slurm_drmaa() -> types.ModuleType("drmaa"):
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # TODO [MIC-4469]: make more generic for external users
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa
