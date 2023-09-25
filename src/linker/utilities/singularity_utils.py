import subprocess
from pathlib import Path
from typing import List

from loguru import logger


def run_with_singularity(input_data: List[Path], results_dir: Path, step_dir: Path) -> None:
    logger.info("Running container with singularity")
    _run_container(input_data, results_dir, step_dir)
    _clean(results_dir, step_dir)


def _run_container(input_data: List[Path], results_dir: Path, step_dir: Path) -> None:
    cmd = f"singularity run --pwd {step_dir} --bind {results_dir}:/results "
    for filepath in input_data:
        cmd += f"--bind {str(filepath)}:/input_data/{str(filepath.name)} "
    cmd += f"{step_dir}/image.sif"
    logger.info("Running the singularity container")
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


def _clean(results_dir: Path, step_dir: Path) -> None:
    pass
    # TODO: do I need to clean up the cache?
