import subprocess
from pathlib import Path
from typing import List

from loguru import logger


def run_with_singularity(
    input_data: List[Path],
    results_dir: Path,
    container_path: Path,
    step_name: str,
    implementation_name: str,
) -> None:
    logger.info("Running container with singularity")
    # TODO [MIC-4708]: break this singularity implementation_dir hard-coding
    ## Need to figure out how to add files to singualrity container rather than
    ## changing the pwd of the container
    implementation_dir = (
        Path(__file__).parent.parent
        / "steps"
        / step_name
        / "implementations"
        / implementation_name
    )
    _run_container(input_data, results_dir, implementation_dir, container_path)
    _clean(results_dir, implementation_dir, container_path)


def _run_container(
    input_data: List[Path], results_dir: Path, implementation_dir: Path, container_path: Path
) -> None:
    cmd = f"singularity run --pwd {implementation_dir} --bind /tmp:/tmp --bind {results_dir}:/results "
    for filepath in input_data:
        cmd += f"--bind {str(filepath)}:/input_data/{str(filepath.name)} "
    cmd += f"{container_path}"
    _run_cmd(results_dir, cmd)


def _run_cmd(results_dir: Path, cmd: str) -> None:
    logger.debug(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    process = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Error running command '{cmd}'\n" f"Error: {process.stderr}")

    with (results_dir / "singularity.o").open(mode="a") as output_file:
        output_file.write(f"{process.stdout}\n")
        output_file.write(process.stderr)


def _clean(results_dir: Path, implementation_dir: Path, container_path: Path) -> None:
    pass
    # TODO: do I need to clean up the cache?
