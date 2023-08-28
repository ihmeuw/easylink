from pathlib import Path

from loguru import logger
import shutil
from typing import Dict, Any

from linker.utilities.docker_utils import run_with_docker
from linker.utilities.env_utils import get_compute_config
from linker.utilities.pipeline_utils import get_steps
from linker.utilities.singularity_utils import run_with_singularity


def main(
    pipeline_specification: Path,
    container_engine: str,
    computing_environment: str,
    results_dir: Path,
):
    step_dir = get_steps(pipeline_specification)
    compute_config = get_compute_config(computing_environment)

    if compute_config["computing_environment"] == "local":
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
        launch_slurm_job(compute_config)

    else:
        raise NotImplementedError(
            "only --computing-invironment 'local' is supported; "
            f"provided '{computing_environment}'"
        )

def launch_slurm_job(compute_config: Dict[str, Dict]):
    resources = compute_config["computing_environment"]["slurm"]["implementation_resources"]
    breakpoint()
    drmaa = _get_drmaa()
    # with drmaa.Session() as s:
    #     jt = s.createJobTemplate()
    #     jt.remoteCommand = shutil.which("linker")
    #     jt.args = ["run"]
    #     jt.outputPath = f":{str(cluster_logging_root / '%A.%a.log')}"
    #     jt.errorPath = f":{str(cluster_logging_root / '%A.%a.log')}"
    #     jt.jobEnvironment = {
    #         "LC_ALL": "en_US.UTF-8",
    #         "LANG": "en_US.UTF-8",
    #     }
    #     jt.joinFiles = True
    #     jt.nativeSpecification = native_specification.to_cli_args()

    #     job_ids = s.runBulkJobs(jt, 1, num_workers, 1)
    #     array_job_id = job_ids[0].split(".")[0]

    #     def kill_jobs() -> None:
    #         try:
    #             s.control(array_job_id, drmaa.JobControlAction.TERMINATE)
    #         # FIXME: Hack around issue where drmaa.errors sometimes doesn't
    #         #        exist.
    #         except Exception as e:
    #             if "already completing" in str(e) or "Invalid job" in str(e):
    #                 # This is the case where all our workers have already shut down
    #                 # on their own, which isn't actually an error.
    #                 pass
    #             else:
    #                 raise

    #     atexit.register(kill_jobs)

# class NativeSpecification(NamedTuple):
#     job_name: str
#     project: str
#     queue: str
#     peak_memory: str
#     max_runtime: str

#     # Class constant
#     NUM_THREADS: int = 1

#     def to_cli_args(self):
#         return (
#             f"-J {self.job_name} "
#             f"-A {self.project} "
#             f"-p {self.queue} "
#             f"--mem={self.peak_memory*1024} "
#             f"-t {self.max_runtime} "
#             f"-c {self.NUM_THREADS}"
#         )

def _get_drmaa() -> Any:
    """Returns object() to bypass RuntimeError when not on a DRMAA-compliant system"""
    try:
        import drmaa
    except (RuntimeError, OSError):
        # if "slurm" in ENV_VARIABLES.HOSTNAME.value:
        #     ENV_VARIABLES.DRMAA_LIB_PATH.update("/opt/slurm-drmaa/lib/libdrmaa.so")
        #     import drmaa
        # else:
        drmaa = object()
    return drmaa
