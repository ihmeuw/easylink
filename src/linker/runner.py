import os
import shutil
import socket
from pathlib import Path
from typing import Any, Dict, NamedTuple

from loguru import logger

from linker.utilities.cli_utils import prepare_results_directory
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.env_utils import get_compute_config
from linker.utilities.pipeline_utils import get_steps
from linker.utilities.singularity_utils import run_with_singularity


def main(
    pipeline_specification: Path,
    container_engine: str,
    computing_environment: str,
):
    step_dir = get_steps(pipeline_specification)
    compute_config = get_compute_config(computing_environment)

    if compute_config["computing_environment"] == "local":
        # NOTE: All variations of running the pipeline converge to `--computing-environment local`
        # and so we define the results directory there
        results_dir = prepare_results_directory(
            pipeline_specification, computing_environment
        )
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
    elif [k for k in compute_config["computing_environment"]][0] == "slurm":
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        launch_slurm_job(
            pipeline_specification,
            container_engine,
            compute_config,
        )

    else:
        raise NotImplementedError(
            "only --computing-invironment 'local' is supported; "
            f"provided '{computing_environment}'"
        )


def launch_slurm_job(
    pipeline_specification: Path,
    container_engine: str,
    results_dir: Path,
    compute_config: Dict[str, Dict],
    launch_time: str,
) -> None:
    resources = compute_config["computing_environment"]["slurm"][
        "implementation_resources"
    ]
    drmaa = get_slurm_drmaa()
    s = drmaa.Session()
    s.initialize()
    jt = s.createJobTemplate()
    jt.jobName = f"linker_run_{launch_time}"
    jt.joinFiles = False  # keeps stdout separate from stderr
    jt.outputPath = f":{str(results_dir / '%A.o%a')}"
    jt.errorPath = f":{str(results_dir / '%A.e%a')}"
    jt.remoteCommand = shutil.which("linker")
    jt.args = [
        "run",
        str(pipeline_specification),
        "--container-engine",
        container_engine,
        "--computing-environment",
        "local",  # Running 'local' on landed node
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


def get_slurm_drmaa() -> Any:
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        import drmaa

    return drmaa
